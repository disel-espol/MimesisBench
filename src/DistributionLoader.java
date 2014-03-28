package org.apache.hadoop.fs.nnmetadata;

import java.util.Random;

import org.apache.commons.configuration.DataConfiguration;
import org.apache.commons.configuration.Configuration;
//import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang.ArrayUtils;

/**
 * Loads statistical distributions (histograms) from text files.
 */
public class DistributionLoader {
  
  // to fit tail
  public final String tailClusterKey = "FIRST_CLUSTER_IN_TAIL";
  public final int tailClusterDefault = -1; // no tail (implicit in other clusters)
  public int tailCluster;
    
  // workload generation time, in milliseconds
  public final static String maxTimeKey = "MAX_TIME";
  public final static long maxTimeDefault = 1 * 60 * 60 * 1000; // 1 hour
  public long maxTime;
  
  // Statistics regarding operations other than opens, creates, and deletes:  
  // 1) Of those accesses to regular files, what percentage are listStatus (vs. opens)
  public final String ratioOfFileLsToOpensKey = "RATIO_OF_FILE_LS_TO_OPENS";
  public final double ratioOfFileLsToOpensDefault = 0; // By default, all regular accesses are open events
  public double ratioOfFileLsToOpens;
  // 2) How frequent are ls on directories vs. regular accesses? Note: Regular access = opens + fileListStatus
  public final String ratioOfDirLsToAccessesKey = "RATIO_OF_DIR_LS_TO_ACCESSES";
  public final double ratioOfDirLsToAccessesDefault = 0; // By default, we do no listStatus on directories
  public double ratioOfDirLsToAccesses;
  // 3) How frequent are mkdirs vs. creates?
  public final String ratioOfMkdirsToCreatesKey = "RATIO_OF_MKDIRS_TO_CREATES";
  public final double ratioOfMkdirsToCreatesDefault = 0; // By default, we don't issue mkdir operations
  public double ratioOfMkdirsToCreates;
  // 4) How frequent are renames of files vs. creates?
  public final String ratioOfFileRenamesToCreatesKey = "RATIO_OF_FILE_RENAMES_TO_CREATES";
  public final double ratioOfFileRenamesToCreatesDefault = 0; // By default, we do no renames
  public double ratioOfFileRenamesToCreates;
  // 5) How frequent are renames of directories vs. creates?
  public final String ratioOfDirRenamesToCreatesKey = "RATIO_OF_DIR_RENAMES_TO_CREATES";
  public final double ratioOfDirRenamesToCreatesDefault = 0; // By default, we do no renames
  public double ratioOfDirRenamesToCreates;
  
  // number of file types
  public final String numFileTypesKey = "FILE_TYPES";
  public final int numFileTypesDefault = 7;
  public int numFileTypes;
  
  // target number of create events
  public final String targetCreatesKey = "TARGET_CREATES";
  public final int targetCreatesDefault = 100000;
  public int targetCreates;
    
  //public final String preExistingFSKey = "PRE_EXISTING_FS";
  //public String preExistingFS;
  public final String preExistingFilesKey = "PRE_EXISTING_FILES";
  public int preExistingFiles;
  
  // File types
  public FileType[] fileTypes = null;
  public Long[] fileTypesKeys;
  public final String fileTypesWeightsKey = "CLUSTER_WEIGHTS";
  public double[] fileTypesWeights;
  
  // Files at each depth in the hierarchy
  public String filesAtDepthKeysKey = "FILES_AT_DEPTH_KEYS";
  public Long[] filesAtDepthKeys;
  public String filesAtDepthWeightsKey = "FILES_AT_DEPTH_WEIGHTS";
  public double[] filesAtDepthWeights;
  
  DataConfiguration config = null;
  private boolean persistConfig = false;
  
  public DistributionLoader(Configuration c, int fileType)
  {
    config = new DataConfiguration(c) ;

    this.maxTime = config.getLong(this.maxTimeKey, this.maxTimeDefault);
    //this.preExistingFS = config.getString(this.preExistingFSKey, "");
    this.numFileTypes = config.getInt(this.numFileTypesKey, this.numFileTypesDefault);
    this.targetCreates = config.getInt(this.targetCreatesKey, this.targetCreatesDefault);
    
    this.ratioOfFileLsToOpens = config.getDouble(this.ratioOfFileLsToOpensKey, this.ratioOfFileLsToOpensDefault);
    this.ratioOfDirLsToAccesses = config.getDouble(this.ratioOfDirLsToAccessesKey, this.ratioOfDirLsToAccessesDefault);
    this.ratioOfMkdirsToCreates = config.getDouble(this.ratioOfMkdirsToCreatesKey, this.ratioOfMkdirsToCreatesDefault);
    this.ratioOfFileRenamesToCreates = config.getDouble(this.ratioOfFileRenamesToCreatesKey, this.ratioOfFileRenamesToCreatesDefault);
    this.ratioOfDirRenamesToCreates = config.getDouble(this.ratioOfDirRenamesToCreatesKey, this.ratioOfDirRenamesToCreatesDefault);
    
    this.fileTypes = new FileType[numFileTypes];
    this.fileTypesKeys = new Long[numFileTypes];
    this.fileTypesWeights = new double[numFileTypes];
    
    this.filesAtDepthKeys = (Long[]) config.getLongList(filesAtDepthKeysKey).toArray(ArrayUtils.EMPTY_LONG_OBJECT_ARRAY);
    //this.filesAtDepthKeys = (Long[]) (config.getLongList(filesAtDepthKeysKey)).toArray(new Long[0]);
    //this.filesAtDepthKeys = ArrayUtilsconfig.getLongList(this.filesAtDepthKeysKey).toArray(ArrayUtils.EMPTY_LONG_OBJECT_ARRAY);
    this.filesAtDepthWeights = config.getDoubleArray(filesAtDepthWeightsKey);
    
    int i = -1;
    try
    {     
      RandomGenerator<Long> aod;
      RandomGenerator<Long> activation;
      RandomGenerator<Long> deactivation;
      RandomGenerator<Long> openInterarrivals;
      RandomGenerator<Long> perFileOpenInterarrivals;
      RandomGenerator<Long> aoa;
      RandomGenerator<Long> creates;
      
      String openAgeClusterKeysKey;
      long[] openAgeClusterKeys;
      String openAgeClusterWeightsKey;
      double[] openAgeClusterWeights;
      String deleteAgeClusterKeysKey;
      long[] deleteAgeClusterKeys;
      String deleteAgeClusterWeightsKey;
      double[] deleteAgeClusterWeights;
      String createInterClusterKeysKey;
      long[] createInterClusterKeys;
      String createInterClusterWeightsKey;
      double[] createInterClusterWeights;
      String openInterClusterKeysKey;
      long[] openInterClusterKeys;
      String openInterClusterWeightsKey;
      double[] openInterClusterWeights;
      String perFileOpenInterClusterKeysKey;
      long[] perFileOpenInterClusterKeys;
      String perFileOpenInterClusterWeightsKey;
      double[] perFileOpenInterClusterWeights;
      String activationClusterKeysKey;
      long[] activationClusterKeys;
      String activationClusterWeightsKey;
      double[] activationClusterWeights;
      String deactivationClusterKeysKey;
      long[] deactivationClusterKeys;
      String deactivationClusterWeightsKey;
      double[] deactivationClusterWeights;
      fileTypesWeights = config.getDoubleArray(fileTypesWeightsKey);
      
      if (fileType >= 0 && fileType < this.numFileTypes)
      {
        i = fileType;
        
        // get file type config
        int cluster = (int) fileType + 1;

        deleteAgeClusterKeysKey = "AOD_CLUSTER" + cluster + "_KEYS";
        deleteAgeClusterKeys = config.getLongArray(deleteAgeClusterKeysKey);
        deleteAgeClusterWeightsKey = "AOD_CLUSTER" + cluster + "_WEIGHTS";
        deleteAgeClusterWeights = config.getDoubleArray(deleteAgeClusterWeightsKey);
        
        createInterClusterKeysKey = "CREATE_INTERARRIVAL_CLUSTER" + cluster + "_KEYS";
        createInterClusterKeys = config.getLongArray(createInterClusterKeysKey);
        createInterClusterWeightsKey = "CREATE_INTERARRIVAL_CLUSTER" + cluster + "_WEIGHTS";
        createInterClusterWeights = config.getDoubleArray(createInterClusterWeightsKey);

        perFileOpenInterClusterKeysKey = "OPEN_INTERARRIVAL_CLUSTER" + cluster + "_PER_FILE_KEYS";
        perFileOpenInterClusterKeys = config.getLongArray(perFileOpenInterClusterKeysKey);
        perFileOpenInterClusterWeightsKey = "OPEN_INTERARRIVAL_CLUSTER" + cluster + "_PER_FILE_WEIGHTS";
        perFileOpenInterClusterWeights = config.getDoubleArray(perFileOpenInterClusterWeightsKey);

        activationClusterKeysKey = "ACTIVATION_TIME_CLUSTER" + cluster + "_KEYS";
        activationClusterKeys = config.getLongArray(activationClusterKeysKey);
        activationClusterWeightsKey = "ACTIVATION_TIME_CLUSTER" + cluster + "_WEIGHTS";
        activationClusterWeights = config.getDoubleArray(activationClusterWeightsKey);

        deactivationClusterKeysKey = "DEACTIVATION_TIME_CLUSTER" + cluster + "_KEYS";
        deactivationClusterKeys = config.getLongArray(deactivationClusterKeysKey);
        deactivationClusterWeightsKey = "DEACTIVATION_TIME_CLUSTER" + cluster + "_WEIGHTS";
        deactivationClusterWeights = config.getDoubleArray(deactivationClusterWeightsKey);
        
        preExistingFiles = config.getInt(this.preExistingFilesKey + cluster, 0);
        
        // create the distributions (random generators)
        Long seed = System.currentTimeMillis();
        Random r = new Random(NNMetadataMapper.getRandomSeed());

        aod =  null;
        aoa = null;
        openInterarrivals = null;
        perFileOpenInterarrivals = null;
        activation = null;      
        deactivation = null;
        creates = null; 
        
        if (cluster >= this.tailCluster && this.tailCluster > 0)
        {
          aod =  new WeightedRandomGenerator(new Random(r.nextLong()), ArrayUtils.toObject(deleteAgeClusterKeys), deleteAgeClusterWeights);
          perFileOpenInterarrivals = new WeightedRandomGenerator(new Random(r.nextLong()), ArrayUtils.toObject(perFileOpenInterClusterKeys), perFileOpenInterClusterWeights);
          activation = new WeightedRandomGenerator(new Random(r.nextLong()), ArrayUtils.toObject(activationClusterKeys), activationClusterWeights);      
          deactivation = new WeightedRandomGenerator(new Random(r.nextLong()), ArrayUtils.toObject(deactivationClusterKeys), deactivationClusterWeights);
          creates = new WeightedRandomGenerator(new Random(r.nextLong()), ArrayUtils.toObject(createInterClusterKeys), createInterClusterWeights); 
        } else {
          aod =  new WeightedTriangularRandomGenerator(new Random(r.nextLong()), deleteAgeClusterKeys, deleteAgeClusterWeights, true);
          perFileOpenInterarrivals = new WeightedTriangularRandomGenerator(new Random(r.nextLong()), perFileOpenInterClusterKeys, perFileOpenInterClusterWeights, true);
          activation = new WeightedTriangularRandomGenerator(new Random(r.nextLong()), activationClusterKeys, activationClusterWeights, true);     
          deactivation = new WeightedTriangularRandomGenerator(new Random(r.nextLong()), deactivationClusterKeys, deactivationClusterWeights, true);
          creates = new WeightedTriangularRandomGenerator(new Random(r.nextLong()), createInterClusterKeys, createInterClusterWeights, true); 
        }
        // create the file types
        fileTypes[i] = new FileType(i, aod, activation, deactivation, creates, perFileOpenInterarrivals);
        fileTypesKeys[i] = (long)i;
      }
      
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
      System.exit(0);
    }
  }
  
  public static long getMaxBenchmarkTime(Configuration c) {
    DataConfiguration config = new DataConfiguration(c) ;
    return config.getLong(maxTimeKey, maxTimeDefault);
  }
}
