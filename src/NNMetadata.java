package org.apache.hadoop.fs.nnmetadata;

import java.io.IOException;
import java.util.Date;
import java.io.DataInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.File;
import java.io.BufferedReader;
import java.util.StringTokenizer;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Iterator;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This program executes a specified operation that applies realistic namespace
 * load to the NameNode. It was developed as a modification of NNBench, so parts of the 
 * code may overlap with parts of NNBench.
 * 
 * Valid operations are:
 *   create_write
 *   open_read
 *   rename
 *   delete
 * 
 * NOTE: The open_read, rename and delete operations assume that the files
 *       they operate on are already available. The create_write operation 
 *       must be run before running the other operations.
 */
public class NNMetadata {
  private static final Log LOG = LogFactory.getLog(
          NNMetadata.class);
  
  protected static String CONTROL_DIR_NAME = "control";
  protected static String OUTPUT_DIR_NAME = "output";
  protected static String INI_OUTPUT_DIR_NAME = "initialization_output";
  protected static String DATA_DIR_NAME = "data";
  protected static final String DEFAULT_RES_FILE_NAME = "NNMetadata_results.log";
  protected static final String NNMetadata_VERSION = "NameNode Metadata Benchmark 0.1";
  
  public static String operation = "none";
  public static long numberOfMaps = 1l; // default is 1
  public static long numberOfReduces = 1l; // default is 1
  public static long startTime = 
          System.currentTimeMillis() + (120 * 1000); // default is 'now' + 2min
  public static long blockSize = 128 * 1024 * 1024l; // 128 MB //1l; // default is 1
  public static int bytesToWrite = 0; // default is 0
  public static long bytesPerChecksum = 1l; // default is 1
  public static long numberOfFiles = -1l; // default is -1
  public static short replicationFactorPerFile = 3; //1; // default is 1
  public static String baseDir = "/Benchmarks/NNMetadata";  // default
  public static boolean readFileAfterOpen = false; // default is to not read
  public static long randomSeed = 1234l; // default is 1234
  public static boolean useHierarchicalNamespace = false;
  public static String hierarchyFilepath = null;
  
  // Supported operations
  protected static final String OP_CREATE_WRITE = "create_write";
  protected static final String OP_OPEN_READ = "open_read";
  protected static final String OP_RENAME = "rename";
  protected static final String OP_DELETE = "delete";
  
  // To display in the format that matches the NN and DN log format
  // Example: 2007-10-26 00:01:19,853
  static SimpleDateFormat sdf = 
          new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss','S");

  private static Configuration config = new Configuration();
  
  /**
   * Clean up the files before a test run
   * 
   * @throws IOException on error
   */
  private static void cleanupBeforeTestrun() throws IOException {
    FileSystem tempFS = FileSystem.get(config);
    
    // Delete the data directory only if it is the create/write operation
    if (operation.equals(OP_CREATE_WRITE)) {
      LOG.info("Deleting data directory");
      tempFS.delete(new Path(baseDir, DATA_DIR_NAME), true);
    }
    tempFS.delete(new Path(baseDir, CONTROL_DIR_NAME), true);
    tempFS.delete(new Path(baseDir, OUTPUT_DIR_NAME), true);
  }
  
  /**
   * Create control files before a test run.
   * Number of files created is equal to the number of maps specified
   * 
   * @throws IOException on error
   */
  private static void createControlFiles() throws IOException {
    FileSystem tempFS = FileSystem.get(config);
    LOG.info("Creating " + numberOfMaps + " control files");
    boolean useSequentialLoadGenerator = config.getBoolean("test.nnmetadata.usesequentialloadgenerator", false);

    Path filePath = null; 
    String prev = "-1";
    
    if (useHierarchicalNamespace) {
      // Create the namespace hierarchy
      filePath = new Path(hierarchyFilepath);
      BufferedReader reader = new BufferedReader(new InputStreamReader(tempFS.open(filePath)));
      
      String line;
      // Assumes sorted order of file
      // TO DO: Abort if not sorted
      while((line = reader.readLine()) != null) {
        if (!line.startsWith(prev) && prev.compareTo("-1") != 0) {
          tempFS.mkdirs(new Path(prev));
        }
        prev = line;        
      }
      // deal with last line
      tempFS.mkdirs(new Path(prev));
    }
    
    for (int i = 0; i < numberOfMaps + ((useSequentialLoadGenerator) ? 1 : 0); i++) {
      String strFileName = "NNMetadata_Controlfile_" + i;
      filePath = new Path(new Path(baseDir, CONTROL_DIR_NAME),
              strFileName);

      SequenceFile.Writer writer = null;
      try {
        writer = SequenceFile.createWriter(tempFS, config, filePath, Text.class, 
                LongWritable.class, CompressionType.NONE);
        // NOTE: Current implementation supports one file type (each with 1 or more files in it)
        // TO DO: One mapper should be able to support multiple file types. Otherwise,
        // this limits number of file types to MAX SLOTS in cluster.
        
        if (i < numberOfMaps) {
          writer.append(new Text("CREATES PRODUCER"), new LongWritable((long) i)); // PRODUCER of creates (and delete) events
          writer.append(new Text("OPENS PRODUCER"), new LongWritable((long) i)); // PRODUCER of open events
          writer.append(new Text("CONSUMER"), new LongWritable((long) i)); // CONSUMER
        } else {
          // this is the sequential load generator
          // multithreading issues do not affect the latency readings of this generator
          writer.append(new Text("SEQUENTIAL LOAD GENERATOR"), new LongWritable((long) i));
        }
      } finally {
        if (writer != null) {
          writer.close();
        }
      }
    }
  }

  /**
   * Display version
   */
  private static void displayVersion() {
    System.out.println(NNMetadata_VERSION);
  }
  
  /**
   * Display usage
   */
  private static void displayUsage() {
    String usage = "Pending...";
    
    System.out.println(usage);
  }

  /**
   * check for arguments and fail if the values are not specified
   * @param index  positional number of an argument in the list of command
   *   line's arguments
   * @param length total number of arguments
   */
  public static void checkArgs(final int index, final int length) {
    if (index == length) {
      displayUsage();
      System.exit(-1);
    }
  }
  
  /**
   * Parse input arguments
   *
   * @param args array of command line's parameters to be parsed
   */
  public static void parseInputs(final String[] args) {
    // If there are no command line arguments, exit
    if (args.length == 0) {
      displayUsage();
      System.exit(-1);
    }
    
    // Parse command line args
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-operation")) {
        operation = args[++i];
      } else if (args[i].equals("-maps")) {
        checkArgs(i + 1, args.length);
        numberOfMaps = Long.parseLong(args[++i]);
      } else if (args[i].equals("-reduces")) {
        checkArgs(i + 1, args.length);
        numberOfReduces = Long.parseLong(args[++i]);
      } else if (args[i].equals("-startTime")) {
        checkArgs(i + 1, args.length);
        startTime = Long.parseLong(args[++i]) * 1000;
      } else if (args[i].equals("-blockSize")) {
        checkArgs(i + 1, args.length);
        blockSize = Long.parseLong(args[++i]);
      } else if (args[i].equals("-bytesToWrite")) {
        checkArgs(i + 1, args.length);
        bytesToWrite = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-bytesPerChecksum")) {
        checkArgs(i + 1, args.length);
        bytesPerChecksum = Long.parseLong(args[++i]);
      } else if (args[i].equals("-numberOfFiles")) {
        checkArgs(i + 1, args.length);
        //numberOfFiles = Long.parseLong(args[++i]);
      } else if (args[i].equals("-replicationFactorPerFile")) {
        checkArgs(i + 1, args.length);
        replicationFactorPerFile = Short.parseShort(args[++i]);
      } else if (args[i].equals("-baseDir")) {
        checkArgs(i + 1, args.length);
        baseDir = args[++i];
      } else if (args[i].equals("-readFileAfterOpen")) {
        checkArgs(i + 1, args.length);
        readFileAfterOpen = Boolean.parseBoolean(args[++i]);
      } else if (args[i].equals("-randomSeed")) {
        checkArgs(i + 1, args.length);
        randomSeed = Long.parseLong(args[++i]);
      } else if (args[i].equals("-help")) {
        displayUsage();
        System.exit(-1);
      }
    }
    
    LOG.info("Test Inputs: ");
    LOG.info("           Test Operation: " + operation);
    LOG.info("               Start time: " + sdf.format(new Date(startTime)));
    LOG.info("           Number of maps: " + numberOfMaps);
    LOG.info("        Number of reduces: " + numberOfReduces);
    LOG.info("               Block Size: " + blockSize);
    LOG.info("           Bytes to write: " + bytesToWrite);
    LOG.info("       Bytes per checksum: " + bytesPerChecksum);
    LOG.info("       Replication factor: " + replicationFactorPerFile);
    LOG.info("                 Base dir: " + baseDir);
    LOG.info("     Read file after open: " + readFileAfterOpen);
        
    // Set user-defined parameters, so the map method can access the values
    config.set("test.nnmetadata.operation", operation);
    config.setLong("test.nnmetadata.maps", numberOfMaps);
    config.setLong("test.nnmetadata.reduces", numberOfReduces);
    config.setLong("test.nnmetadata.starttime", startTime);
    config.setLong("test.nnmetadata.blocksize", blockSize);
    config.setInt("test.nnmetadata.bytestowrite", bytesToWrite);
    config.setLong("test.nnmetadata.bytesperchecksum", bytesPerChecksum);
    config.setInt("test.nnmetadata.replicationfactor", 
            (int) replicationFactorPerFile);
    config.set("test.nnmetadata.basedir", baseDir);
    config.setBoolean("test.nnmetadata.readFileAfterOpen", readFileAfterOpen);
    config.setLong("test.nnmetadata.randomseed", randomSeed);
    
    config.set("test.nnmetadata.datadir.name", DATA_DIR_NAME);
    config.set("test.nnmetadata.outputdir.name", OUTPUT_DIR_NAME);
    config.set("test.nnmetadata.controldir.name", CONTROL_DIR_NAME);
    
    useHierarchicalNamespace = config.getBoolean("test.nnmetadata.usehierarchicalnamespace", false);
    if (useHierarchicalNamespace)
      hierarchyFilepath = config.get("test.nnmetadata.conffile", "mimesis.properties") + ".namespace";

  }
  
  private static void getResultForAttribute(String prefix, String attr, String value, long[] results) {        
    String opString = attr.substring(attr.indexOf(prefix) + prefix.length(), attr.length() - 1); // removes the prefix and the "s" at the end of attr
    results[Enum.valueOf(MyFile.EventType.class, opString).ordinal()] = Long.parseLong(value);
  }
  
  /**
   * Analyze the results
   * 
   * @throws IOException on error
   */
  private static void analyzeResults() throws IOException {
    final FileSystem fs = FileSystem.get(config);
    Path reduceFile = new Path(new Path(baseDir, OUTPUT_DIR_NAME),
            "part-r-00000");
    System.out.println("Opening output file: " + reduceFile.toString());

    DataInputStream in;
    in = new DataInputStream(fs.open(reduceFile));

    BufferedReader lines;
    lines = new BufferedReader(new InputStreamReader(in));

    long[] totalTimeAL1 = new long[MyFile.EventType.values().length];
    long[] totalTimeAL2 = new long[MyFile.EventType.values().length];
    long totalTimeTPmS = 0l;
    long lateMaps = 0l;
    long[] numOfExceptions = new long[MyFile.EventType.values().length];
    long[] successfulFileOps = new long[MyFile.EventType.values().length];
    
    long mapStartTimeTPmS = 0l;
    long mapEndTimeTPmS = 0l;
    long mapActiveTimeTPmS = 0l;
    
    String resultTPSLine1 = null;
    String resultTPSLine2 = null;
    String resultALLine1 = null;
    String resultALLine2 = null;
    
    String line;
    
    while((line = lines.readLine()) != null) {
      StringTokenizer tokens = new StringTokenizer(line, " \t\n\r\f%;");
      String attr = tokens.nextToken().toUpperCase();
      
      if (attr.contains(":TOTALTIMEAL1")) {
        getResultForAttribute(":TOTALTIMEAL1", attr, tokens.nextToken(), totalTimeAL1); 
      } else if (attr.contains(":TOTALTIMEAL2")) {
        getResultForAttribute(":TOTALTIMEAL2", attr, tokens.nextToken(), totalTimeAL2);
      } else if (attr.endsWith(":TOTALTIMETPMS")) {
        totalTimeTPmS = Long.parseLong(tokens.nextToken());
      } else if (attr.endsWith(":LATEMAPS")) {
        lateMaps = Long.parseLong(tokens.nextToken());
      } else if (attr.contains(":NUMOFEXCEPTIONS")) {
        getResultForAttribute(":NUMOFEXCEPTIONS", attr, tokens.nextToken(), numOfExceptions);
      } else if (attr.contains(":SUCCESSFUL")) {
        getResultForAttribute(":SUCCESSFUL", attr, tokens.nextToken(), successfulFileOps);
      } else if (attr.endsWith(":MAPSTARTTIMETPMS")) {
        mapStartTimeTPmS = Long.parseLong(tokens.nextToken());
      } else if (attr.endsWith(":MAPENDTIMETPMS")) {
        mapEndTimeTPmS = Long.parseLong(tokens.nextToken());
      } else if (attr.endsWith(":MAPACTIVETIMETPMS")) {
        mapActiveTimeTPmS = Long.parseLong(tokens.nextToken());
      }
    }

    long sumSuccessfulFileOps = 0;
    long sumNumOfExceptions = 0;

    for (int i = 0; i < successfulFileOps.length ; i++) {
      sumSuccessfulFileOps += successfulFileOps[i];
      sumNumOfExceptions += numOfExceptions[i];
    }

    // The time it takes for the longest running map is measured. Using that,
    // cluster transactions per second is calculated. It includes time to 
    // retry any of the failed operations
    double longestMapTimeTPmS = (double) (mapEndTimeTPmS - mapStartTimeTPmS);
    double totalTimeTPS = (longestMapTimeTPmS == 0) ?
            (1000 * sumSuccessfulFileOps) :
            (double) (1000 * sumSuccessfulFileOps) / longestMapTimeTPmS;
            
    // The time it takes to perform 'n' operations is calculated (in ms),
    // n being the number of files. Using that time, the average execution 
    // time is calculated. It includes time to retry any of the
    // failed operations
    double AverageExecutionTime = (totalTimeTPmS == 0) ?
        (double) sumSuccessfulFileOps : 
        (double) totalTimeTPmS / sumSuccessfulFileOps;
    
    String resultLines[] = {
    "-------------- NNMetadata -------------- : ",
    "                               Version: " + NNMetadata_VERSION,
    "                           Date & time: " + sdf.format(new Date(
            System.currentTimeMillis())),
    "",
    "                            Start time: " + 
      sdf.format(new Date(startTime)),
    "                           Maps to run: " + numberOfMaps,
    "                        Reduces to run: " + numberOfReduces,
    "                    Block Size (bytes): " + blockSize,
    "                        Bytes to write: " + bytesToWrite,
    "                    Bytes per checksum: " + bytesPerChecksum,
    "                       Number of files: " + numberOfFiles,
    "                    Replication factor: " + replicationFactorPerFile,
    "        # maps that missed the barrier: " + lateMaps,
    "",
    "                 RAW DATA: AL Total #1: " + totalTimeAL1,
    "                 RAW DATA: AL Total #2: " + totalTimeAL2,
    "              RAW DATA: TPS Total (ms): " + totalTimeTPmS,
    "       RAW DATA: Longest Map Time (ms): " + longestMapTimeTPmS,
    "                   RAW DATA: Late maps: " + lateMaps,
    "             RAW DATA: # of exceptions: " + sumNumOfExceptions,
    "             RAW DATA: active time: " + mapActiveTimeTPmS,
    "             Opens / second: " + (successfulFileOps[MyFile.EventType.OPEN.ordinal()] / (mapActiveTimeTPmS / 1000)),
    "" };

    PrintStream res = new PrintStream(new FileOutputStream(
            new File(DEFAULT_RES_FILE_NAME), true));
    
    // Write to a file and also dump to log
    System.out.println("Printing stats: ");
    for(int i = 0; i < resultLines.length; i++) {
      LOG.info(resultLines[i]);
      res.println(resultLines[i]);
    }

    // Now, print operation-specific stats
    for (MyFile.EventType eventType : MyFile.EventType.values()) {
      int eventIndex = eventType.ordinal();
      String separator = System.getProperty( "line.separator" );
          
      // Average latency is the average time to perform 'n' number of
      // operations, n being the number of files
      double avgLatency1 = (double) totalTimeAL1[eventIndex] / successfulFileOps[eventIndex];
      double avgLatency2 = (double) totalTimeAL2[eventIndex] / successfulFileOps[eventIndex];
      resultALLine1 = "";
      resultALLine2 = "";
      
      if (eventType == MyFile.EventType.CREATE) {
        // For create/write/close, it is treated as two transactions,
        // since a file create from a client perspective involves create and close
        resultALLine1 = "            Avg Lat (ms): Create/Write: " + avgLatency1;
        resultALLine2 = "                   Avg Lat (ms): Close: " + avgLatency2;
      } else if (eventType == MyFile.EventType.OPEN) {
        resultALLine1 = "                    Avg Lat (ms): Open: " + avgLatency1;
        if (readFileAfterOpen) {
          resultALLine2 = "                  Avg Lat (ms): Read: " + avgLatency2;
        }
      } else if (eventType == MyFile.EventType.DELETE) {
        resultALLine1 = "                  Avg Lat (ms): Delete: " + avgLatency1;
      }
      String outLines = "                        Test Operation: " + eventType.toString() + separator +
          "            Successful file operations: " + successfulFileOps[eventIndex] + separator +
          "" + separator +
          "                          # exceptions: " + numOfExceptions[eventIndex] + separator +
          "" + separator +
          resultALLine1 + separator +
          resultALLine2 + separator +
          "";
      
      // Write to a file and also dump to log
      LOG.info(outLines);
      res.println(outLines);    
    }

    res.close();
  }
  
  /**
   * Run the test
   * 
   * @throws IOException on error
   */
  public static void runTests() throws IOException, InterruptedException, ClassNotFoundException {
    config.setLong("io.bytes.per.checksum", bytesPerChecksum);
    
    Cluster cluster = new Cluster(config);
    
    // First, create the pre-existing namespace
    Job namespaceIntializer = Job.getInstance(cluster, config);
    namespaceIntializer.setJarByClass(NNMetadata.class);
    namespaceIntializer.setJobName("NNMetadata-NamespaceInitializer");
    
    FileInputFormat.setInputPaths(namespaceIntializer, new Path(baseDir, CONTROL_DIR_NAME));
    namespaceIntializer.setInputFormatClass(SequenceFileInputFormat.class);
    namespaceIntializer.setMaxMapAttempts(1);    
    namespaceIntializer.setSpeculativeExecution(false);
    namespaceIntializer.setMapperClass(CreatePreExistingNamespaceMapper.class);
    namespaceIntializer.setReducerClass(IntSumReducer.class);
    FileOutputFormat.setOutputPath(namespaceIntializer, new Path(baseDir, INI_OUTPUT_DIR_NAME));
    namespaceIntializer.setOutputKeyClass(Text.class);
    namespaceIntializer.setOutputValueClass(IntWritable.class);
    namespaceIntializer.setNumReduceTasks(1);
    
    if (!namespaceIntializer.waitForCompletion(true))
      System.exit(-1);

    
    // set the start time for the mappers
    config.setLong("test.nnmetadata.starttime", System.currentTimeMillis() + (90 * 1000)); // default is 'now' + 1.5min
    
    // Now, create the workload
    Job job = Job.getInstance(cluster, config);
    job.setJarByClass(NNMetadata.class);
    job.setJobName("NNMetadata-" + operation);
    FileInputFormat.setInputPaths(job, new Path(baseDir, CONTROL_DIR_NAME));
    job.setInputFormatClass(SequenceFileInputFormat.class);
    
    // Explicitly set number of max map attempts to 1.
    job.setMaxMapAttempts(1);
    
    // Explicitly turn off speculative execution
    job.setSpeculativeExecution(false);
    
    job.setMapperClass(MultithreadedMapper.class);
    MultithreadedMapper.setMapperClass(job, NNMetadataMapper.class);
    MultithreadedMapper.setNumberOfThreads(job, 3);

    job.setReducerClass(NNMetadataReducer.class);

    FileOutputFormat.setOutputPath(job, new Path(baseDir, OUTPUT_DIR_NAME));
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks((int) numberOfReduces);
    
    if (!job.waitForCompletion(true))
      System.exit(-1);
  }
  
  /**
   * Validate the inputs
   */
  public static void validateInputs() {
    // If it is not one of the four operations, then fail
    if (!operation.equals(OP_CREATE_WRITE) &&
            !operation.equals(OP_OPEN_READ) &&
            !operation.equals(OP_RENAME) &&
            !operation.equals(OP_DELETE)) {
      System.err.println("Error: Unknown operation: " + operation);
      displayUsage();
      System.exit(-1);
    }
    
    // If number of maps is a negative number, then fail
    // Hadoop allows the number of maps to be 0
    if (numberOfMaps < 0) {
      System.err.println("Error: Number of maps must be a positive number");
      displayUsage();
      System.exit(-1);
    }
    
    // If number of reduces is a negative number or 0, then fail
    if (numberOfReduces <= 0) {
      System.err.println("Error: Number of reduces must be a positive number");
      displayUsage();
      System.exit(-1);
    }

    // If blocksize is a negative number or 0, then fail
    if (blockSize <= 0) {
      System.err.println("Error: Block size must be a positive number");
      displayUsage();
      System.exit(-1);
    }
    
    // If bytes to write is a negative number, then fail
    if (bytesToWrite < 0) {
      System.err.println("Error: Bytes to write must be a positive number");
      displayUsage();
      System.exit(-1);
    }
    
    // If bytes per checksum is a negative number, then fail
    if (bytesPerChecksum < 0) {
      System.err.println("Error: Bytes per checksum must be a positive number");
      displayUsage();
      System.exit(-1);
    }
    
    // If replication factor is a negative number, then fail
    if (replicationFactorPerFile < 0) {
      System.err.println("Error: Replication factor must be a positive number");
      displayUsage();
      System.exit(-1);
    }
    
    // If block size is not a multiple of bytesperchecksum, fail
    if (blockSize % bytesPerChecksum != 0) {
      System.err.println("Error: Block Size in bytes must be a multiple of " +
              "bytes per checksum: ");
      displayUsage();
      System.exit(-1);
    }
  }

  /**
  * Main method for running the NNMetadata Metadatamarks
  *
  * @param args array of command line arguments
  * @throws IOException indicates a problem with test startup
  */
  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    // Display the application version string
    displayVersion();

    // Parse the inputs
    parseInputs(args);
    
    // Validate inputs
    validateInputs();
    
    // Clean up files before the test run
    cleanupBeforeTestrun();
    
    // Create control files before test run
    createControlFiles();

    // Run the tests as a map reduce job
    runTests();
    
    // Analyze results
    analyzeResults();
  }
}

