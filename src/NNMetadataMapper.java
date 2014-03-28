package org.apache.hadoop.fs.nnmetadata;

import java.io.IOException;
import java.util.Date;
import java.io.DataInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.File;
import java.io.BufferedReader;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Random;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.ConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Mapper class
 */
public class NNMetadataMapper extends Mapper<Text, LongWritable, Text, Text> {
      
  private static final Log LOG = LogFactory.getLog(
      NNMetadataMapper.class);
  
  protected static EventGenerator gen;
  private static boolean isInitialized = false;
  private static boolean useHierarchicalNamespace = false;
  private String name;
  
  private static Context context = null;
  
  private static long randomSeed = -1;
  private static final long defaultRandomSeed = 1234;
  
  protected static NamespaceHierarchyManager hierarchyManager = null;
  
  /**
   * Constructor
   */
  public NNMetadataMapper() {
    gen = new EventGenerator();
  }

  public static long getRandomSeed() {
    if (randomSeed < 0)
      return defaultRandomSeed;
    
    return randomSeed;
  }
  
  @Override
  public void setup(Context context) throws IOException, InterruptedException {

  }

  /**
   * Returns when the current number of seconds from the epoch equals
   * the command line argument given by <code>-startTime</code>.
   * This allows multiple instances of this program, running on clock
   * synchronized nodes, to start at roughly the same time.
   * @return true if the method was able to sleep for <code>-startTime</code>
   * without interruption; false otherwise
   */
  private boolean barrier() {
    long startTime = context.getConfiguration().getLong("test.nnmetadata.starttime", 0l);
    long currentTime = System.currentTimeMillis(); 
    long sleepTime = startTime - currentTime;
    boolean retVal = false;

    // If the sleep time is greater than 0, then sleep and return
    if (sleepTime > 0) {
      LOG.info("Waiting in barrier for: " + sleepTime + " ms; " + this.name);

      try {
        Thread.sleep(sleepTime);
        retVal = true;
      } catch (Exception e) {
        retVal = false;
      }
    }
    
    LOG.info("Exiting barrier; " + this.name);

    return retVal;
  }

  private static void loadPreExistingHierarchy() {
    if (isInitialized)
      throw new RuntimeException("Invalid attempt to load pre-existing hierarchy when NNMetadatamapper has already been initialized.");
    if (!useHierarchicalNamespace)
      throw new RuntimeException("Invalid attempt to load pre-existing hierarchy when useHierarchicalNamespace = false.");
       
    hierarchyManager = new NamespaceHierarchyManager(gen, context.getConfiguration());
    hierarchyManager.load();
    gen.setHierarchyManager(hierarchyManager);
  }
  
  private synchronized static void initialize(int fileType, int mapperID, Context c) {
    if (isInitialized)
      return;

    context = c;
    
    Configuration conf = context.getConfiguration();

    randomSeed = conf.getLong("test.nnmetadata.randomseed", 12345);
      
    useHierarchicalNamespace = conf.getBoolean("test.nnmetadata.usehierarchicalnamespace", false);
    
    // Read file type/cluster config parameters 
    PropertiesConfiguration distributionsConfig = null;
        
    try {
      Path configFilePath = new Path(conf.get("test.nnmetadata.conffile", "mimesis.properties") + ".common");
      FileSystem fs = FileSystem.get(new Configuration());
      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(configFilePath)));
      distributionsConfig = new PropertiesConfiguration();
      distributionsConfig.load(br);
      br.close();

      configFilePath = new Path(conf.get("test.nnmetadata.conffile", "mimesis.properties") + "." + Integer.toString(fileType + 1));
      br = new BufferedReader(new InputStreamReader(fs.open(configFilePath)));
      distributionsConfig.load(br);
      br.close();      
    } catch (Exception e) {
      if (fileType + 1 < context.getConfiguration().getLong("test.nnmetadata.maps", 0l)) {
        String warnString = "Unable to load configuration file: " + conf.get("test.nnmetadata.conffile", "mimesis.properties") + "." + Integer.toString(fileType) +
            "; original exception follows:\n" + e.getMessage() + "\n" + e.toString();
        LOG.warn(warnString);
        throw new RuntimeException(warnString);
      }
      
      //gen.setMaxBenchmarkTime(DistributionLoader.getMaxBenchmarkTime(distributionsConfig));
      gen.setMaxBenchmarkTime(context.getConfiguration().getLong("test.nnmetadata.sequentialtesttime", 0l));
      distributionsConfig = null;      
    }        

    gen.setContext(context);
    
    gen.setFileType(fileType, mapperID, distributionsConfig, conf);

    if (useHierarchicalNamespace) {
      loadPreExistingHierarchy();
    }

    isInitialized = true;
  }
  
  /**
   * Map method
   */ 
  @Override
  public void map(Text key, 
      LongWritable value,
      Context context) throws IOException, InterruptedException {
    
    // Find out which file type has been assigned to this mapper.
    // This is encoded in the file name as follows: NNMetadata_Controlfile_TYPE ,
    // where the TYPE is a number between 0 and NUM_FILE_TYPES - 1
    
    long totalTimeTPmS = 0l;
    long startTimeTPmS = 0l;
    long activeTimeTPmS = 0l;
    long endTimeTPms = 0l;
    
    int mapperID = (int) value.get();
    int fileType;
    
    if (context.getConfiguration().getBoolean("test.nnmetadata.useASingleFileType", false)) {
      fileType = context.getConfiguration().getInt("test.nnmetadata.FixedFileType", -1) - 1;
    } else {
      fileType = mapperID;
    }
    
    this.name = key.toString();
    
    initialize(fileType, mapperID, context);
        
    if (barrier()) {
      if (key.toString().compareTo("SEQUENTIAL LOAD GENERATOR") == 0) {
        gen.issueSequentialEvents();
        
        // collect stats for sequential opens
        context.write(new Text("l:totalTimeAL1openSingleThreads"), 
            new Text(String.valueOf(gen.getTotalTimeAL1(MyFile.EventType.OPEN))));
        context.write(new Text("l:totalTimeAL2openSingleThreads"), 
            new Text(String.valueOf(gen.getTotalTimeAL2(MyFile.EventType.OPEN))));
        context.write(new Text("l:numOfExceptionsOpenSingleThreads"), 
            new Text(String.valueOf(gen.getNumOfExceptions(MyFile.EventType.OPEN))));
        context.write(new Text("l:successfulOpenSingleThreads"), 
            new Text(String.valueOf(gen.getSuccessfulFileOps(MyFile.EventType.OPEN))));
      } else if (key.toString().compareTo("CREATES PRODUCER") == 0) {
        // Decrease the priority of the producer; then, launch producer
        Thread.currentThread().setPriority(Thread.NORM_PRIORITY - 2);
        
        gen.prepareCreateEvents();
        endTimeTPms = System.currentTimeMillis();
      } else if (key.toString().compareTo("OPENS PRODUCER") == 0) {
        // Decrease the priority of the producer; then, launch producer
        Thread.currentThread().setPriority(Thread.NORM_PRIORITY - 2);
        
        gen.prepareOpenEvents();
        endTimeTPms = System.currentTimeMillis();
      } else if (key.toString().compareTo("CONSUMER") == 0) {
        // Increase the priority of the sonsumer; then, launch consumer
        Thread.currentThread().setPriority(Thread.NORM_PRIORITY + 2);
        
        startTimeTPmS = System.currentTimeMillis();
        gen.issueEvents(); 
        
        //Cannot exit until scheduler has finished scheduling events
        while (!gen.isDone()) {
          Thread.sleep(10000); // sleep for 10 seconds
        }
        endTimeTPms = System.currentTimeMillis(); // TO DO: FIX! this may be off by as much as 10 seconds
        activeTimeTPmS = gen.getActiveTime();
        
        // collect after the map end time is measured
        context.write(new Text("l:totalTimeAL1creates"), 
            new Text(String.valueOf(gen.getTotalTimeAL1(MyFile.EventType.CREATE))));
        context.write(new Text("l:totalTimeAL2creates"), 
            new Text(String.valueOf(gen.getTotalTimeAL2(MyFile.EventType.CREATE))));
        context.write(new Text("l:numOfExceptionsCreates"), 
            new Text(String.valueOf(gen.getNumOfExceptions(MyFile.EventType.CREATE))));
        context.write(new Text("l:successfulCreates"), 
            new Text(String.valueOf(gen.getSuccessfulFileOps(MyFile.EventType.CREATE))));
        
        context.write(new Text("l:totalTimeAL1opens"), 
            new Text(String.valueOf(gen.getTotalTimeAL1(MyFile.EventType.OPEN))));
        context.write(new Text("l:totalTimeAL2opens"), 
            new Text(String.valueOf(gen.getTotalTimeAL2(MyFile.EventType.OPEN))));
        context.write(new Text("l:numOfExceptionsOpens"), 
            new Text(String.valueOf(gen.getNumOfExceptions(MyFile.EventType.OPEN))));
        context.write(new Text("l:successfulOpens"), 
            new Text(String.valueOf(gen.getSuccessfulFileOps(MyFile.EventType.OPEN))));

        context.write(new Text("l:totalTimeAL1deletes"), 
            new Text(String.valueOf(gen.getTotalTimeAL1(MyFile.EventType.DELETE))));
        context.write(new Text("l:totalTimeAL2deletes"), 
            new Text(String.valueOf(gen.getTotalTimeAL2(MyFile.EventType.DELETE))));
        context.write(new Text("l:numOfExceptionsDeletes"), 
            new Text(String.valueOf(gen.getNumOfExceptions(MyFile.EventType.DELETE))));
        context.write(new Text("l:successfulDeletes"), 
            new Text(String.valueOf(gen.getSuccessfulFileOps(MyFile.EventType.DELETE))));

        context.write(new Text("l:totalTimeTPmS"), 
                new Text(String.valueOf(totalTimeTPmS)));
        context.write(new Text("min:mapStartTimeTPmS"), 
            new Text(String.valueOf(startTimeTPmS)));
        context.write(new Text("max:mapEndTimeTPmS"), 
            new Text(String.valueOf(endTimeTPms)));
        context.write(new Text("max:mapActiveTimeTPmS"), 
            new Text(String.valueOf(activeTimeTPmS)));
      } else {
        throw new IllegalStateException("Thread is not CONSUMER OR PRODUCER: " + key.toString());
      }
    }    
  }
}
