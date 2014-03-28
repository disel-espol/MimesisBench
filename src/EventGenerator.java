package org.apache.hadoop.fs.nnmetadata;

import java.io.IOError;
import java.io.IOException;
import java.io.StringWriter;
import java.io.PrintWriter;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.Random;
import java.util.Set;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class EventGenerator {
  private static int DEFAULT_EVENT_QUEUE_SIZE = 1500000;
  private static long ISSUE_AHEAD_TIME = 1; // 1 msec
  private static long MAX_ALLOWED_DRIFT = 2000; // 2 seconds
  
  private static BlockingQueue<NamespaceEvent> eventsToBeIssued = null;
  private static PriorityBlockingQueue<MyFile> files = null;
  private static Set<String> deletedFiles = null;
  //private static Set<String> renamedFilesOrDirs = null;
  private static ThreadMXBean bean = null;
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(20);
  private Context context = null;

  public String baseDir;
  public String dataDirName;
  public short replFactor;
  public long blkSize;
  public double replaySpeedFactor;
  public double replaySpeedFactorDefault = 1; // replay as fast as original; 0 --> replay as fast as possible
  public boolean useHierarchicalNamespace = false;
  
  private FileSystem filesystem = null;
  
  private static boolean doneCreatingPreExistingNamespace = false;
  private static boolean doneCreatingFiles = false;   
  private static boolean doneCreatingEvents = false;
  private int maxCreateEvents = 0; 
  private int preExistingFilesOfCurrentType = 0;
  private long startTime = -1;
  private long endTime = -1;
  private FileType fileType = null;
  private int mapperID = -1;
  private int numFileTypes = -1;
  private double[] fileTypesWeights;
  private long maxTime = -1;
  private int filesCreated = 0;
  private Long[] filesAtDepthKeys;
  private double[] filesAtDepthWeights;
  private NamespaceHierarchyManager hierarchyManager = null;
  
  private static double ratioOfFileLsToOpens;
  private static double ratioOfDirLsToAccesses;
  private static double ratioOfMkdirsToCreates;
  private static double ratioOfFileRenamesToCreates;
  private static double ratioOfDirRenamesToCreates;
  
  public static WeightedCoin fileLsToOpensCoin;
  public static WeightedCoin dirLsToAccessesCoin;
  public static WeightedCoin mkdirsToCreatesCoin;
  public static WeightedCoin fileRenamesToCreatesCoin;
  public static WeightedCoin dirRenamesToCreatesCoin;
  
  //stats
  private static int numOfExceptionsCreates = 0;
  private static int numOfExceptionsOpens = 0;
  private static int numOfExceptionsDeletes = 0;
  private static int numOfExceptionsListStatus = 0;
  private static int numOfExceptionsMkdirs = 0;
  private static int numOfExceptionsRenames = 0;
  private static long totalTimeAL1creates = 0l;
  private static long totalTimeAL2creates = 0l;
  private static long totalTimeAL1opens = 0l;
  private static long totalTimeAL2opens = 0l;
  private static long totalTimeAL1deletes = 0l;
  private static long totalTimeAL2deletes = 0l;
  private static long totalTimeAL1listStatus = 0l;
  private static long totalTimeAL2listStatus = 0l;
  private static long totalTimeAL1mkdirs = 0l;
  private static long totalTimeAL2mkdirs = 0l;
  private static long totalTimeAL1renames = 0l;
  private static long totalTimeAL2renames = 0l;
  private static long successfulCreates = 0l;
  private static long successfulOpens = 0l;
  private static long successfulDeletes = 0l;
  private static long successfulListStatus = 0l;
  private static long successfulMkdirs = 0l;
  private static long successfulRenames = 0l;
  
  private static Exception haltException = null;
  private static boolean exceptionOccurred = false;
  
  private static int[] filesOfCurrentTypeAtDepthOrBelow = null;
  private WeightedRandomGenerator weightedDepthGen = null;
  
  private static final Log LOG = LogFactory.getLog(
      EventGenerator.class);
  
  public EventGenerator() {
        
    if (eventsToBeIssued == null) {
      eventsToBeIssued = new ArrayBlockingQueue<NamespaceEvent>(DEFAULT_EVENT_QUEUE_SIZE);
      
      deletedFiles = Collections.newSetFromMap(new ConcurrentHashMap<String,Boolean>());
      //renamedFilesOrDirs = Collections.newSetFromMap(new ConcurrentHashMap<String,Boolean>());

      bean = ManagementFactory.getThreadMXBean();
      if ( ! bean.isThreadCpuTimeSupported() ) {
        throw new RuntimeException("isThreadCpuTimeSupported() is not supported in this system.");
      }
      
      bean.setThreadCpuTimeEnabled(true);
      
      if ( ! bean.isThreadContentionMonitoringSupported() ) {
        throw new RuntimeException("isThreadContentionMonitoringSupported() is not supported in this system.");
      }

      bean.setThreadContentionMonitoringEnabled(true);
    }
  }
  
  public int getNumFileTypes() {
    return this.numFileTypes;
  }
  
  public int getFileTypeName() {
    return this.fileType.getName();
  }
  
  public int getMapperID() {
    return this.mapperID;
  }
  
  public double getFileTypeWeight(int type) {
    return this.fileTypesWeights[type];
  }
  
  protected void setEndTime(long t) {
    this.endTime = t;
  }
  
  public synchronized void setContext(Context c) {
    if (context != null) {
      if (c != context)
        throw new UnsupportedOperationException("Context cannot be changed once it is set.");
      
      return;
    }
    
    context = c;
  }
  
  public synchronized void setFileType(int t, int m, PropertiesConfiguration distributionsConfig, Configuration conf) {
    if (fileType != null) {
      if (t != fileType.getName())
        throw new UnsupportedOperationException("File type cannot be changed once it is set.");
      
      return;
    }
    
    try {
      filesystem = FileSystem.get(conf);
    } catch(Exception e) {
      throw new RuntimeException("Cannot get file system.", e);
    }
    
    if (distributionsConfig != null) {
      DistributionLoader loader = new DistributionLoader(distributionsConfig, t);
      maxCreateEvents = (int) Math.round(loader.targetCreates * loader.fileTypesWeights[t]);
      preExistingFilesOfCurrentType = loader.preExistingFiles;
      filesAtDepthKeys = loader.filesAtDepthKeys;
      filesAtDepthWeights = loader.filesAtDepthWeights;
      weightedDepthGen = new WeightedRandomGenerator(new Random(m), filesAtDepthKeys, filesAtDepthWeights); //TODO: Consider making the random generator non-deterministic (remove fixed seed)
      filesCreated = 0;
      
      try {
        if (files == null) {
            files = new PriorityBlockingQueue<MyFile>(preExistingFilesOfCurrentType + maxCreateEvents + 1, new Comparator<MyFile>() {
            public int compare(MyFile thisFile, MyFile otherFile) {
              
              Long stamp1 = thisFile.getNextEventStamp();
              Long stamp2 = otherFile.getNextEventStamp();
    
              // ascending order
              int returnValue = (stamp1.compareTo(stamp2) != 0) ?  stamp1.compareTo(stamp2) : (thisFile.getID() - otherFile.getID());
              
              if (returnValue == 0 && thisFile.getNextEventType() != otherFile.getNextEventType()) {
                // For two operations on the same file, CREATES should come before OPENS/DELETES and DELETES should come after CREATES/OPENS
                if (thisFile.getNextEventType() == MyFile.EventType.CREATE || otherFile.getNextEventType() == MyFile.EventType.DELETE)
                  returnValue = -1;
                else if (thisFile.getNextEventType() == MyFile.EventType.DELETE || otherFile.getNextEventType() == MyFile.EventType.CREATE)
                  returnValue = 1;
                else if (thisFile.getNextEventType() == MyFile.EventType.RENAME) // renames come before opens
                  returnValue = -1;
                else if (otherFile.getNextEventType() == MyFile.EventType.RENAME) // renames come before opens
                  returnValue = 1;
              }
              return returnValue;
            }
          } );
        }
      } catch(Exception e) {
        throw new RuntimeException("Problem creating a PiorityBlockingQueue with capacity " + maxCreateEvents + "(" + loader.targetCreates +
            "*" + loader.fileTypesWeights[t]+ "); fileType: " + t + "; original exception follows:\n" +
            e.getMessage() + "\n" + e.toString());
      }
      
      fileType = loader.fileTypes[t];
      mapperID = m;
      numFileTypes = loader.numFileTypes;
      fileTypesWeights = loader.fileTypesWeights;
      
      maxTime = loader.maxTime;
      
      ratioOfFileLsToOpens = loader.ratioOfFileLsToOpens;
      ratioOfDirLsToAccesses = loader.ratioOfDirLsToAccesses;
      ratioOfMkdirsToCreates = loader.ratioOfMkdirsToCreates;
      ratioOfFileRenamesToCreates = loader.ratioOfFileRenamesToCreates;
      ratioOfDirRenamesToCreates = loader.ratioOfDirRenamesToCreates;
      
      fileLsToOpensCoin = new WeightedCoin(ratioOfFileLsToOpens, new Random((int) (ratioOfFileLsToOpens * 1000)));
      dirLsToAccessesCoin = new WeightedCoin(ratioOfDirLsToAccesses, new Random((int) (ratioOfDirLsToAccesses * 1000)));
      mkdirsToCreatesCoin = new WeightedCoin(ratioOfMkdirsToCreates, new Random((int) (ratioOfMkdirsToCreates * 1000)));
      fileRenamesToCreatesCoin = new WeightedCoin(ratioOfFileRenamesToCreates, new Random((int) (ratioOfFileRenamesToCreates * 1000)));
      dirRenamesToCreatesCoin = new WeightedCoin(ratioOfDirRenamesToCreates, new Random((int) (ratioOfDirRenamesToCreates * 1000)));
    }
    
    baseDir = conf.get("test.nnmetadata.basedir");
    dataDirName = conf.get("test.nnmetadata.datadir.name");
    blkSize = conf.getLong("test.nnmetadata.blocksize", 1l);
    replFactor = (short) (conf.getInt("test.nnmetadata.replicationfactor", 1));
    useHierarchicalNamespace = conf.getBoolean("test.nnmetadata.usehierarchicalnamespace", false);
    
    replaySpeedFactor = conf.getDouble("test.nnmetadata.replayspeedfactor", replaySpeedFactorDefault);
       
    if (useHierarchicalNamespace) {
      filesOfCurrentTypeAtDepthOrBelow = new int[filesAtDepthKeys.length];
      double accum = 0;
      for (int i = 0; i < filesAtDepthKeys.length; i++) {
        filesOfCurrentTypeAtDepthOrBelow[i] = (int) Math.round(filesAtDepthWeights[i] * preExistingFilesOfCurrentType);
        
        // Fix rounding error
        accum += filesAtDepthWeights[i] * preExistingFilesOfCurrentType - Math.round(filesAtDepthWeights[i] * preExistingFilesOfCurrentType);
        if (accum > 0.99 && filesAtDepthWeights[i] > 0) {
          filesOfCurrentTypeAtDepthOrBelow[i] += 1;
          accum = 0;
        }
        if (i > 0)
          filesOfCurrentTypeAtDepthOrBelow[i] += filesOfCurrentTypeAtDepthOrBelow[i - 1]; 
      }
      
      // Fix rounding errors (unlikely to be needed)
      int idx = filesAtDepthKeys.length - 1;
      int largestVal = filesOfCurrentTypeAtDepthOrBelow[idx];
      while (filesOfCurrentTypeAtDepthOrBelow[idx] == largestVal && largestVal < preExistingFilesOfCurrentType) {
        filesOfCurrentTypeAtDepthOrBelow[idx] = preExistingFilesOfCurrentType;
        idx -= 1;
        if (idx < 0)
          break;
      }
    }
   
    if (fileType != null) {
      LOG.info("Setting file type: " + fileType.getName() + "; " + baseDir + "/" + dataDirName);
    } else {
      LOG.info("Base dir and data dir: " + baseDir + "/" + dataDirName + "; max becnharmking time: " + maxTime);
    }
  }
  
  public void setMaxBenchmarkTime(long t) {
    maxTime = t;
  }
  
  public void setHierarchyManager(NamespaceHierarchyManager manager) {
    hierarchyManager = manager;    
  }
  
  public int createPreExistingNamespace() {
    if (fileType == null)
      throw new IllegalStateException("Cannot operate on an unknown file type; call setFileType first.");
    
    int numEvents = 0;
    long currTime = 0;
    
    MyFile newFile = null;
    
    LOG.info("Preparing pre-existing namespace; queue: " + eventsToBeIssued.hashCode() + "; preExistingFilesOfCurrentType = " + preExistingFilesOfCurrentType);
    
    // Calculate how many files should be located at each depth
    int i;
        
    int currentDepthIndex = 0;
    for (i = 0; i < preExistingFilesOfCurrentType; i++) {
      // determine the base path for the file
      String base = "";
      
      if (!useHierarchicalNamespace) {
        base = baseDir + "/" + dataDirName + "/";
      } else {        
        // Determine to which depth this file belongs to
        while (filesOfCurrentTypeAtDepthOrBelow[currentDepthIndex] <= i)
          currentDepthIndex += 1;

        base = getBaseDir(i, currentDepthIndex);
      }
      
      // create the file
      String name = mapperID + "_" + i;
        
      scheduler.schedule(new ScheduledEvent(base + name, MyFile.EventType.CREATE, this), 0, TimeUnit.MILLISECONDS);
      context.progress();      
    }
    
    doneCreatingPreExistingNamespace = true;

    //Will not issue more events
    scheduler.shutdown(); // Initiates an orderly shutdown in which previously submitted tasks are executed, but no new tasks will be accepted.
    
    LOG.info("Done creating pre-existing namespace; preExistingFilesOfCurrentType = " + preExistingFilesOfCurrentType +
        "; scheduler.isTerminated() = " + scheduler.isTerminated());
    
    return preExistingFilesOfCurrentType;
  }
  
  private String getBaseDir(int fileID, int depthIndex) {
    if (!useHierarchicalNamespace) {
      return baseDir + "/" + dataDirName + "/";
    }     
    
    // Pick a directory at depth - 1; the file itself will be located at the target depth
    // NOTE: The depth in the hierarchy is not currentDepthIndex,
    //       but rather filesAtDepthKeys[currentDepthIndex]
    int parentDepth = filesAtDepthKeys[depthIndex].intValue() - 1;
    return hierarchyManager.getDirAtDepthForFile(parentDepth, fileID);
  }
  
  /**
   * PRODUCER of create (and delete) events.
   */
  public void prepareCreateEvents() {
    if (fileType == null)
      throw new IllegalStateException("Cannot operate on an unknown file type; call setFileType first.");
    
    int numEvents = 0;
    long currTime = 0;
    String name = null;
    
    MyFile newFile = null;
    
    LOG.info("Preparing create events.; queue: " + eventsToBeIssued.hashCode() + "; maxCreateEvents = " + maxCreateEvents);
    
    // First, add the pre-existing files to the files data structure
    int i;
    int depthIndex = -1;
    for (i = 0; i < preExistingFilesOfCurrentType; i++) {
      if (i % 1000 == 0) // Don't want to log too many messages !
        LOG.info("Adding pre-existing file " + i);
      
      if (!useHierarchicalNamespace) {
        depthIndex = -1;
      } else {
        depthIndex = Arrays.binarySearch(filesOfCurrentTypeAtDepthOrBelow, i + 1);
        
        if (depthIndex < 0)
          depthIndex = -1 * depthIndex - 1; //2; // binarySearch returns a negative val if exact # not found; details in javadoc
  
        // This may not be the right depth; fix if necessary
        while (filesAtDepthWeights[depthIndex] == 0 ) {//&& depthIndex > 0) {
          depthIndex -= 1;
       // NOTE: For current data, should not reach 0!
          if (depthIndex < 0) {
            hierarchyManager = null;
            System.gc();
            throw new RuntimeException("DEBUG: " + depthIndex + " " + fileType.getName() + "_" + i +
                "\n" + Arrays.toString(filesAtDepthKeys) + 
                "\n" + Arrays.toString(filesAtDepthWeights) +
                "\n" + Arrays.toString(filesOfCurrentTypeAtDepthOrBelow));
          }
        }
      }
      
      name = mapperID + "_" + i;
      
      newFile = new MyFile(fileType, name, i, getBaseDir(i, depthIndex));
      files.put(newFile);
      context.progress();
    }
    
    filesCreated = preExistingFilesOfCurrentType;
    
    // Now, add the files to be created during the benchmark
    for (i = preExistingFilesOfCurrentType; i < (preExistingFilesOfCurrentType + maxCreateEvents); i++) {
      if (i % 1000 == 0) // Don't want to log too many messages !
        LOG.info("Adding file " + i);      

      //  create the file
      filesCreated++;
      
      if (!useHierarchicalNamespace) {
        depthIndex = -1;
      } else {
        // Random weighted selection of the depth index
        depthIndex = weightedDepthGen.next().intValue();
      }
      
      name = mapperID + "_" + i;
      
      newFile = new MyFile(fileType, currTime, name, i, getBaseDir(i, depthIndex));

      currTime = newFile.getCreationStamp();
      
      // add file to queue; block if queue is full
      files.put(newFile);
      
      context.progress();
      
      if (currTime > maxTime) {
        LOG.warn("Will not create more files because currTime > maxTime; currTime = " + currTime + "; maxTime = " + maxTime + "; files.size() = " +
            files.size() + "; eventsToBeIssued.size() = " + eventsToBeIssued.size() + "; scheduler.isTerminated() = " + scheduler.isTerminated() + "\n" +
            "preExistingFilesOfCurrentType = " + preExistingFilesOfCurrentType + "; maxCreateEvents = " + maxCreateEvents);
        break;
      }
    }
    
    doneCreatingFiles = true;
    LOG.info("Done creating files; " + eventsToBeIssued.hashCode() + "; maxCreateEvents = " + maxCreateEvents);
  }
  
  /**
   * PRODUCER of open and lsStatus events (to files, not directories). 
        // Puts operations into a buffer ...
        // have a data structure that contains all files in cluster, sorted by "next event timestamp"
        // then, process by next event timestamp and result of each processing is to enqueue an outgoing event in a
        // fixed capacity queue.
   */
  public void prepareOpenEvents() throws InterruptedException {
    if (fileType == null)
      throw new IllegalStateException("Cannot operate on an unknown file type; call setFileType first.");
    
    long currTime = 0;
    
    LOG.info("Preparing open events.; queue: " + eventsToBeIssued.hashCode() + "; " + doneCreatingFiles + " " + files.isEmpty());
    
    MyFile file = null;
    while (! doneCreatingFiles) {
      Thread.yield(); // if we don't wait for all files to be created, then opens may be added (and processed) from data structure too soon.
                      // TODO: this may cause memory problems. There may be more lightways to accomplish this.
    }
    
    while (!files.isEmpty()) {
      if (currTime > maxTime) {
        LOG.warn("Will not issue open events for remaining files because currTime > maxTime; currTime = " + currTime + "; maxTime = " + maxTime + "; files.size() = " +
            files.size() + "; eventsToBeIssued.size() = " + eventsToBeIssued.size() + "; scheduler.isTerminated() = " + scheduler.isTerminated() + "\n" +
            "preExistingFilesOfCurrentType = " + preExistingFilesOfCurrentType + "; maxCreateEvents = " + maxCreateEvents);
        break;
      }
      
      file = files.poll(50L, TimeUnit.SECONDS);
      context.progress();
      
      if (file == null) {
        continue;
      }
      
      //LOG.info("Preparing an open event for file " + file.getName());
      if (currTime > file.getNextEventStamp())
        throw new IllegalStateException("Event to be issued are out of order: " + file.getNextEventType().toString() +
            "; " + file.getName() + "; " + currTime + " vs " + file.getNextEventStamp());
      
      currTime = file.getNextEventStamp();
      
      NamespaceEvent nextEvent = new NamespaceEvent(file);
      
      // add event to queue; block if queue is full
      eventsToBeIssued.put(nextEvent);
      
      if (file.getNextEventType() == MyFile.EventType.DELETE) {
        // No need to consider this file again; it has already been deleted
        continue;
      }

      // ask the file to find out when its next event will occur, then add back to queue
      file.generateNextEvent();
      if (file.getNextEventStamp() <= maxTime) {
        // Only add file back to queue if next event occurs before end of test
        files.put(file);
      }
    }
    
    doneCreatingEvents = true;
    LOG.info("Done preparing open events; doneCreatingFiles = " + doneCreatingFiles + "; files.isEmpty() = " +
        files.isEmpty() + "; eventsToBeIssued.size() = " + eventsToBeIssued.size() + "; scheduler.isTerminated() = " + scheduler.isTerminated());    
  }

  public void issueSequentialEvents() throws InterruptedException {
    FSDataInputStream input;
    
    long threadID = Thread.currentThread().getId();
    long startTimeAL, startTimeWait, startTimeBlocked;
    long endTimeAL, endTimeWait, endTimeBlocked;
    boolean successfulOp = false;

    long startTime = -1,
        currentTime = -1;
    FileStatus[] status = null;
    
    // TODO: How can I make this work with a hierarchical file system?
    if (useHierarchicalNamespace)
      throw new RuntimeException("issueSequentialEvents not supported when using a hierarchical namespace.");
    
    try {
      status = filesystem.listStatus(new Path(baseDir, dataDirName));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
    LOG.info("Starting single-threaded events issuer for " + status.length + " files in " + baseDir + "/" + dataDirName);
    
    // opean each of the files once (if time allows)
    for (int i = 0; i < status.length ; i++){

      if (startTime < 0) {
        startTime = System.currentTimeMillis();
      }
      
      context.progress();
      
      currentTime = System.currentTimeMillis();
      
      // stop opening files if test time has already been exceeded
      if ((currentTime - startTime) > maxTime)
        break;
      
      try {
        startTimeAL = bean.getCurrentThreadCpuTime();
        startTimeWait = bean.getThreadInfo(threadID).getWaitedTime();

        input = filesystem.open(status[i].getPath());

        endTimeWait = bean.getThreadInfo(threadID).getWaitedTime();
        endTimeAL = bean.getCurrentThreadCpuTime();

        long cTime = System.currentTimeMillis();
        totalTimeAL1opens += (endTimeAL - startTimeAL) / 1000000
            + (endTimeWait - startTimeWait);

        input.close();
        successfulOp = true;
        successfulOpens++;
      } catch (IOException e) {
        numOfExceptionsOpens++;
        LOG.info("Exception recorded in op: OpenRead " + e);
      }
    }           
  }
  
  /**
   * Reads operations from buffer and schedules them to be issued at the desired time. 
   */
  public void issueEvents() throws InterruptedException {
    if (fileType == null)
      throw new IllegalStateException("Cannot operate on an unknown file type; call setFileType first.");
    
    NamespaceEvent futureEvent = null;
    long prevTimeStamp = 0;
    long delay = 0;
    long currentTime = 0;
    long opensIssuedSoFar = 0;
    long eventsScheduledByThisMapper = 0;

    // Make sure at least one event has been queued up (or no events will be issued)
    while (eventsToBeIssued.isEmpty() && !this.doneCreatingEvents)
    {
      Thread.yield();
    }
    
    startTime = -1;
    
    LOG.info("Issuing events; queue: " + eventsToBeIssued.hashCode());
    
     // TO DO: Potential problem, dequeue before other thread puts an earlier event. However, this is not happening in tests, since
    //        I have included a validation for out of order events which would stop the execution with an exception.
    while (! this.doneCreatingEvents || ! eventsToBeIssued.isEmpty() )
    {
      if (startTime < 0) {
        startTime = System.currentTimeMillis();
        prevTimeStamp = startTime;
      }
      
      futureEvent = eventsToBeIssued.poll(50L, TimeUnit.SECONDS);

      context.progress();
      if (futureEvent == null)
        continue;
      
      currentTime = System.currentTimeMillis();
      
      if ((currentTime - startTime) > maxTime) {
        // Test is taking too long. Stop!
        throw new RuntimeException("Test is taking too long! currentTime = " + currentTime + "; maxTime = "
            + maxTime + "; doneCreatingEvents? " + doneCreatingEvents + "; eventsToBeIssued.size() = "
            + eventsToBeIssued.size());
      }
      
      long opDelay = 0;
           
      delay = (startTime + futureEvent.getTimestamp()) - currentTime - ISSUE_AHEAD_TIME;
      
      if (delay < 0) {
        if (delay * (-1) > MAX_ALLOWED_DRIFT) {
            throw new IllegalStateException("Scheduled events are out of order or out of synch: " + prevTimeStamp
                + " vs " + currentTime + "; delay = "+ delay + "; futureEvent.getTimestamp() = " + futureEvent.getTimestamp()
                + "; startTime = " + startTime + "; futureEvent.getEventType().toString() = " + futureEvent.getEventType().toString()
                + "; fileType = " + fileType.getName()
                + "; eventsScheduledByThisMapper = " + eventsScheduledByThisMapper);
        } else {
          delay = 0; // we're already late. Issue as fast as possible!
        }
      }
      
      delay += opDelay;
      
      if (delay > maxTime) {
        // will not schedule event; too far away in the future
        LOG.warn("Will not schedule event; too far away in the future. Stopping event generator. delay: " + delay + "; opDelay: " + opDelay + "; opensIssuedSoFar = " + opensIssuedSoFar +
            "; currentTime = " + currentTime);
        break;
      }
      
      if (futureEvent.getEventType() != MyFile.EventType.OPEN) {
        // Don't log opens because there's too many of them
        LOG.info("Scheduling event "+ futureEvent.hashCode() + ": " + futureEvent.getFilename() + " " + futureEvent.getEventType() + " " + delay);
      } else {
        opensIssuedSoFar += 1;
      }
      
      delay = (long) (delay * replaySpeedFactor);
      
      eventsScheduledByThisMapper += 1;
      scheduler.schedule(new ScheduledEvent(futureEvent.getFilename(), futureEvent.getEventType(), this), delay, TimeUnit.MILLISECONDS);
      
      prevTimeStamp += delay;
      
      if (prevTimeStamp - currentTime > MAX_ALLOWED_DRIFT) {
        LOG.warn("Scheduled events have difted too much from real time: " + prevTimeStamp + " vs " + currentTime + "; opensIssuedSoFar = " + opensIssuedSoFar);
      }
    }
    
    //Will not issue more events
    scheduler.shutdown(); // Initiates an orderly shutdown in which previously submitted tasks are executed, but no new tasks will be accepted.
    
    LOG.info("Done scheduling events; doneCreatingFiles = " + doneCreatingFiles + "; doneCreatingEvents = " + doneCreatingEvents + "; files.isEmpty() = " +
        files.isEmpty() + "; eventsToBeIssued.size() = " + eventsToBeIssued.size() + "; scheduler.isTerminated() = " + scheduler.isTerminated() +
        "; number of open events issued: " + opensIssuedSoFar);
  }
  
  public boolean isDone() {
    context.progress();
    
    if (this.doneCreatingPreExistingNamespace && this.scheduler.isTerminated()) {
      return true;
    }
    
    if (this.doneCreatingEvents && this.doneCreatingFiles && this.eventsToBeIssued.isEmpty() && this.scheduler.isTerminated()) {
      return true;
    }
    
    return false;
  }
  
  public long getActiveTime() {
    if (! isDone())
      return -1;
    return endTime - startTime;
  }
  
  public long getTotalTimeAL1(MyFile.EventType eventType) {
    long totalTimeAL1 = -1;
    
    switch (eventType) {
    case CREATE:
      totalTimeAL1 = totalTimeAL1creates;
      break;
    case DELETE:
      totalTimeAL1 = totalTimeAL1deletes;
      break;
    case OPEN:
      totalTimeAL1 = totalTimeAL1opens;
      break;
    case LISTSTATUS:
      totalTimeAL1 = totalTimeAL1listStatus;
      break;
    case MKDIR:
      totalTimeAL1 = totalTimeAL1mkdirs;
      break;
    case RENAME:
      totalTimeAL1 = totalTimeAL1renames;
      break;
    }
    return totalTimeAL1;
  }
  
  public long getTotalTimeAL2(MyFile.EventType eventType) {
    long totalTimeAL2 = -1;
    
    switch (eventType) {
    case CREATE:
      totalTimeAL2 = totalTimeAL2creates;
      break;
    case DELETE:
      totalTimeAL2 = totalTimeAL2deletes;
      break;
    case OPEN:
      totalTimeAL2 = totalTimeAL2opens;
      break;
    case LISTSTATUS:
      totalTimeAL2 = totalTimeAL2listStatus;
      break;
    case MKDIR:
      totalTimeAL2 = totalTimeAL2mkdirs;
      break;
    case RENAME:
      totalTimeAL2 = totalTimeAL2renames;
      break;
    }
    return totalTimeAL2;
  }
  
  public long getNumOfExceptions(MyFile.EventType eventType) {
    int numOfExceptions = -1;
    
    switch (eventType) {
    case CREATE:
      numOfExceptions = numOfExceptionsCreates;
      break;
    case DELETE:
      numOfExceptions = numOfExceptionsDeletes;
      break;
    case OPEN:
      numOfExceptions = numOfExceptionsOpens;
      break;
    case LISTSTATUS:
      numOfExceptions = numOfExceptionsListStatus;
      break;
    case MKDIR:
      numOfExceptions = numOfExceptionsMkdirs;
      break;
    case RENAME:
      numOfExceptions = numOfExceptionsRenames;
      break;
    }
    return numOfExceptions;
  }
  
  public long getSuccessfulFileOps(MyFile.EventType eventType) {
    long successfulFileOps = -1;
    
    switch (eventType) {
    case CREATE:
      successfulFileOps = successfulCreates;
      break;
    case DELETE:
      successfulFileOps = successfulDeletes;
      break;
    case OPEN:
      successfulFileOps = successfulOpens;
      break;
    case LISTSTATUS:
      successfulFileOps = successfulListStatus;
      break;
    case MKDIR:
      successfulFileOps = successfulMkdirs;
      break;
    case RENAME:
      successfulFileOps = successfulRenames;
      break;
    }
    return successfulFileOps;
  }
  
  public static synchronized void haltExecution(Exception e) {
    haltException = e;
    exceptionOccurred = true;
    throw new RuntimeException(e);
  }
  
  public static synchronized void exitIfExceptionOccurred() {
    //Check if a scheduled task has thrown an exception
    if (exceptionOccurred) {
      throw new RuntimeException(haltException);
    }
  }
  
  class ScheduledEvent implements Runnable {
    private String name = null;
    private MyFile.EventType eventType = null;
    public static final int MAX_OPERATION_EXCEPTIONS = 10; //100;
    private EventGenerator gen;
        
    public ScheduledEvent(String n, MyFile.EventType t, EventGenerator g) {
      name = n;
      eventType = t;
      gen = g;
    }
    
    @Override
    public void run() {
      context.progress();
      
      switch (eventType) {
      case CREATE:
        doCreateOp(name);
        break;
      case DELETE:
        doDeleteOp(name);
        break;
      case OPEN:
        doOpenOp(name);
        break;
      case LISTSTATUS:
        doListStatusOp(name);
        break;
      case MKDIR:
        doMkdirOp(name);
        break;
      case RENAME:
        doRenameOp(name);
        break;
      }
    }
    
    /**
     * Create operation.
     * @param name of the prefix of the putput file to be created
     */
    private void doCreateOp(String name) {
      FSDataOutputStream out;
      byte[] buffer = new byte[0];
      
      Path filePath = new Path(name);

      boolean successfulOp = false;
      long threadID = Thread.currentThread().getId();
      long startTimeAL, startTimeWait, startTimeBlocked;
      long endTimeAL, endTimeWait, endTimeBlocked;
      
      int currentExceptions = 0;

      while (! successfulOp && currentExceptions < MAX_OPERATION_EXCEPTIONS) {
        try {
          // Set up timer for measuring AL (transaction #1)
          startTimeAL = bean.getCurrentThreadCpuTime();
          startTimeWait = bean.getThreadInfo(threadID).getWaitedTime();
          
          // Create the file
          // Use a buffer size of 512
          out = filesystem.create(filePath, 
                  true, 
                  512, 
                  replFactor, 
                  blkSize);
          
          // TODO: Should the write be outside the timing block (aka exclude write from timing)?
          out.write(buffer);
          
          endTimeWait = bean.getThreadInfo(threadID).getWaitedTime();
          endTimeAL = bean.getCurrentThreadCpuTime();
          
          totalTimeAL1creates += (endTimeAL - startTimeAL) / 1000000
              + (endTimeWait - startTimeWait);


          // Close the file / file output stream
          // Set up timers for measuring AL (transaction #2)
          startTimeAL = System.currentTimeMillis();
          out.close();
          
          totalTimeAL2creates += (System.currentTimeMillis() - startTimeAL);
          successfulOp = true;
          successfulCreates++;
        } catch (IOException e) {
          numOfExceptionsCreates++;
          currentExceptions++;
        }
        
      }
    }
    
    /**
     * Open operation
     * @param name of the prefix of the putput file to be opened
     */
    private void doOpenOp(String name) {
      FSDataInputStream input;
      
      Path filePath = new Path(name);
      long threadID = Thread.currentThread().getId();
      long startTimeAL, startTimeWait, startTimeBlocked;
      long endTimeAL, endTimeWait, endTimeBlocked;
      boolean successfulOp = false;
      
      int currentExceptions = 0;
      
      while (! successfulOp && currentExceptions < MAX_OPERATION_EXCEPTIONS) {
        try {
          // Set up timer for measuring AL
          // Currently estimating call time as CPU time + wait time + block time

          
          startTimeAL = bean.getCurrentThreadCpuTime();
          startTimeWait = bean.getThreadInfo(threadID).getWaitedTime();
          
          input = filesystem.open(filePath);
          
          endTimeWait = bean.getThreadInfo(threadID).getWaitedTime();
          endTimeAL = bean.getCurrentThreadCpuTime();
          
          long cTime = System.currentTimeMillis();
          totalTimeAL1opens += (endTimeAL - startTimeAL) / 1000000
              + (endTimeWait - startTimeWait);
          
          input.close();
          successfulOp = true;
          successfulOpens++;

          gen.setEndTime(cTime);

        } catch (IOException e) {
          currentExceptions++;
          numOfExceptionsOpens++;
          
          if (!deletedFiles.contains(name)) // if file has not been deleted yet ==> it has not been created
            Thread.yield(); // to introduce a small delay so that it is more likely that open has already been issued
                           // if still not enough, may want to include an actual delay here
          else
            break;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    /**
     * Delete operation
     * @param name of prefix of the file to be deleted
     */
    private void doDeleteOp(String name) {
      Path filePath = new Path(name);
      
      boolean successfulOp = false;
      long threadID = Thread.currentThread().getId();
      long startTimeAL, startTimeWait, startTimeBlocked;
      long endTimeAL, endTimeWait, endTimeBlocked;
      
      int currentExceptions = 0;
      
      while (! successfulOp && currentExceptions < MAX_OPERATION_EXCEPTIONS) {
        try {
          // Set up timer for measuring AL
          startTimeAL = bean.getCurrentThreadCpuTime();
          startTimeWait = bean.getThreadInfo(threadID).getWaitedTime();

          filesystem.delete(filePath, true);

          endTimeWait = bean.getThreadInfo(threadID).getWaitedTime();
          endTimeAL = bean.getCurrentThreadCpuTime();
          
          //totalTimeAL1deletes += (System.currentTimeMillis() - startTimeAL);
          totalTimeAL1deletes += (endTimeAL - startTimeAL) / 1000000
              + (endTimeWait - startTimeWait);
          
          successfulOp = true;
          successfulDeletes++;
          
          deletedFiles.add(name);
        } catch (IOException e) {
          currentExceptions++;
          numOfExceptionsDeletes++;          
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
    
    /**
     * ListStatus operation
     * @param name of the prefix of the putput file 
     */
    private void doListStatusOp(String name) {
      Path filePath = new Path(name);
      long threadID = Thread.currentThread().getId();
      long startTimeAL, startTimeWait, startTimeBlocked;
      long endTimeAL, endTimeWait, endTimeBlocked;
      boolean successfulOp = false;
      
      int currentExceptions = 0;
      
      while (! successfulOp && currentExceptions < MAX_OPERATION_EXCEPTIONS) {
        try {
          // Set up timer for measuring AL
          // Currently estimating call time as CPU time + wait time + block time
          
          startTimeAL = bean.getCurrentThreadCpuTime();
          startTimeWait = bean.getThreadInfo(threadID).getWaitedTime();
          
          filesystem.listStatus(filePath);
          
          endTimeWait = bean.getThreadInfo(threadID).getWaitedTime();
          endTimeAL = bean.getCurrentThreadCpuTime();
          
          long cTime = System.currentTimeMillis();
          totalTimeAL1listStatus += (endTimeAL - startTimeAL) / 1000000
              + (endTimeWait - startTimeWait);
          
          successfulOp = true;
          successfulListStatus++;

          gen.setEndTime(cTime);

        } catch (IOException e) {
          currentExceptions++;
          numOfExceptionsListStatus++;
          
          if (!deletedFiles.contains(name)) // if file has not been deleted yet ==> it has not been created
            Thread.yield(); // to introduce a small delay so that it is more likely that create has already been issued
                           // if still not enough, may want to include an actual delay here
          else
            break;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
    
    /**
     * Rename operation
     * @param name of the prefix of the putput file 
     */
    private void doRenameOp(String name) {      
      Path filePath = new Path(name);
      Path anotherPath = new Path(name + "R");
      
      long threadID = Thread.currentThread().getId();
      long startTimeAL, startTimeWait, startTimeBlocked;
      long endTimeAL, endTimeWait, endTimeBlocked;
      boolean successfulOp = false;
      
      int currentExceptions = 0;
      
      while (! successfulOp && currentExceptions < MAX_OPERATION_EXCEPTIONS) {
        try {
          // Set up timer for measuring AL
          // Currently estimating call time as CPU time + wait time + block time
          
          startTimeAL = bean.getCurrentThreadCpuTime();
          startTimeWait = bean.getThreadInfo(threadID).getWaitedTime();
          
          filesystem.rename(filePath, anotherPath);
          
          endTimeWait = bean.getThreadInfo(threadID).getWaitedTime();
          endTimeAL = bean.getCurrentThreadCpuTime();
          
          long cTime = System.currentTimeMillis();
          totalTimeAL1renames += (endTimeAL - startTimeAL) / 1000000
              + (endTimeWait - startTimeWait);
          
          successfulOp = true;
          successfulRenames++;

          gen.setEndTime(cTime);

        } catch (IOException e) {
          currentExceptions++;
          numOfExceptionsRenames++;
          
          if (!deletedFiles.contains(name)) // if file has not been deleted yet ==> it has not been created
            Thread.yield(); // to introduce a small delay so that it is more likely that create has already been issued
                           // if still not enough, may want to include an actual delay here
          else
            break;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      
      //NOT USED FOR THE MOMENT:
      //renamedFilesOrDirs.put(k,v);
      //renamedFilesOrDirs.get(k);
      //renamedFilesOrDirs.containsKey(k);
    }
    
    /**
     * Mkdir operation
     * @param name of the prefix of the putput file 
     */
    private void doMkdirOp(String name) {
      // TO DO: Add support for mkdir operations; current release only supports: open, create, delete, listStatus;
      // mkdirs are currently used only in the initialization phase, to create the pre-existing namespace.
      //filesystem.mkdir(filePath);
      //filesystem.mkdirs(filePath); <-- creates parents too
    }
  }
}
