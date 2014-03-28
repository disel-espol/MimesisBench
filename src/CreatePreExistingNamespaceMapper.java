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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Mapper class
 */
public class CreatePreExistingNamespaceMapper extends Mapper<Text, LongWritable, Text, IntWritable> {
      
  private final Log LOG = LogFactory.getLog(
      CreatePreExistingNamespaceMapper.class);
  
  private EventGenerator gen;
  private String name;
  
  private static boolean isInitialized = false;
  private static boolean useHierarchicalNamespace = false;
  
  private Context context = null;
  
  protected static NamespaceHierarchyManager hierarchyManager = null;
  
  /**
   * Constructor
   */
  public CreatePreExistingNamespaceMapper() {
    gen = new EventGenerator();
  }

  private void loadPreExistingHierarchy() {
    if (isInitialized)
      throw new RuntimeException("Invalid attempt to load pre-existing hierarchy when NNMetadatamapper has already been initialized.");
    if (!useHierarchicalNamespace)
      throw new RuntimeException("Invalid attempt to load pre-existing hierarchy when useHierarchicalNamespace = false.");
    
    hierarchyManager = new NamespaceHierarchyManager(gen, context.getConfiguration());
    hierarchyManager.load();
    gen.setHierarchyManager(hierarchyManager);
  }
  
  private void initialize(int fileType, int mapperID, Context c) {
    context = c;
    
    Configuration conf = context.getConfiguration();

    //numberOfFiles = conf.getInt("test.nnmetadata.numberofpreexistingfiles", 1);
    useHierarchicalNamespace = conf.getBoolean("test.nnmetadata.usehierarchicalnamespace", false);
    
    // Read file type/cluster config parameters
    String configFile = null;
    PropertiesConfiguration distributionsConfig = null; 
    try {
      Path configFilePath = new Path(conf.get("test.nnmetadata.conffile", "mimesis.properties") + "." + Integer.toString(fileType + 1));
      FileSystem fs = FileSystem.get(new Configuration());
      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(configFilePath)));
      distributionsConfig = new PropertiesConfiguration();
      distributionsConfig.load(br);
      br.close();
      
      configFilePath = new Path(conf.get("test.nnmetadata.conffile", "mimesis.properties") + ".common");
      br = new BufferedReader(new InputStreamReader(fs.open(configFilePath)));
      distributionsConfig.load(br);
      br.close();
    } catch (Exception e) {     
      String warnString = "Unable to load configuration file: " + conf.get("test.nnmetadata.conffile", "mimesis.properties") + "." + Integer.toString(fileType + 1) +
          "; original exception follows:\n" + e.getMessage() + "\n" + e.toString();
      LOG.warn(warnString);
      throw new RuntimeException(warnString);
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
    
    int mapperID = (int) value.get();
    int fileType;
    
    if (context.getConfiguration().getBoolean("test.nnmetadata.useASingleFileType", false)) {
      fileType = context.getConfiguration().getInt("test.nnmetadata.FixedFileType", -1) - 1;
    } else {
      fileType = mapperID;
    }
    
    this.name = key.toString();

    if (key.toString().compareTo("CREATES PRODUCER") != 0) {
      // No need for this thread; no ops other than creates will be issued now
      return;
    }
    
    initialize(fileType, mapperID, context);
  
    // Create the files
    int createdFiles = gen.createPreExistingNamespace();
    
    //Cannot exit until scheduler has finished scheduling events
    while (!gen.isDone()) {
      Thread.sleep(10000); // sleep for 10 seconds
    }
    context.write(new Text("l:filesCreated"), new IntWritable(createdFiles));
  }  
}
