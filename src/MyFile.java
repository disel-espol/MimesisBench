package org.apache.hadoop.fs.nnmetadata;

import java.util.Comparator;

public class MyFile {
  private FileType fileType = null;
  private long creationStamp = -1;
  private long deletionStamp = -1;
  private long activationStamp = -1;
  private long deactivationStamp = -1;
  private long currTime = -1;
  private int numAccesses = 0;
  private int fileID = -1;
  private String name;
  private String baseDir;
  private boolean willBeRenamed = false;
  private int renameDelay = 1000; // in milliseconds
  
  public enum EventType {
    CREATE, OPEN, DELETE, LISTSTATUS, RENAME, MKDIR, CREATESINGLETHREAD, OPENSINGLETHREAD, DELETESINGLETHREAD   
  }
  
  private EventType nextEventType = null;
  
  public MyFile(FileType t, long ct, String n, int id, String b) {
    fileType = t;
    currTime = ct;
    name = n;
    fileID = id;
    baseDir = b;
    
    creationStamp = currTime + fileType.getNextCreateInterarrival();
    activationStamp = creationStamp + fileType.getNextActivationStamp();
    deactivationStamp = activationStamp + fileType.getNextActiveSpan();
    long span = fileType.getNextSpan();
    
    // Heuristic: make sure file span is not shorter than its active span
    deletionStamp = (creationStamp + span > deactivationStamp ) ? creationStamp + span : deactivationStamp + 1;

    nextEventType = EventType.CREATE;
    
    if (EventGenerator.fileRenamesToCreatesCoin.flip())
      willBeRenamed = true;
    
    currTime = creationStamp;
  }
  
  /**
   * Constructor used for pre-existing files.
   */
  public MyFile(FileType t, String n, int id, String b) {
    fileType = t;
    name = n;
    fileID = id;
    baseDir = b;
    
    creationStamp = -1;
    activationStamp = fileType.getNextActivationStamp();
    deactivationStamp = activationStamp + fileType.getNextActiveSpan();
    long span = fileType.getNextSpan();
    
    // Heuristic: make sure file span is not shorter than its active span
    deletionStamp = (span > deactivationStamp ) ? creationStamp + span : deactivationStamp + 1;

    nextEventType = flipCoinForNextAccess();
    
    currTime = activationStamp;
    numAccesses++; // TO DO: I understand why this is here, but end result may be counting 1 access for files that were never accessed; DOUBLE CHECK
  }
  
  public String getName() {
    return name;
  }
  
  public String getBaseDir() {
    return baseDir;
  }
  
  public int getID() {
    return fileID;
  }

  private EventType flipCoinForNextAccess() {
    // Determine if access should be an open or a listStatus
    if (EventGenerator.fileLsToOpensCoin.flip())
      return EventType.LISTSTATUS;
    
    return EventType.OPEN;
  }
  
  public void generateNextEvent() {
    if (nextEventType == EventType.CREATE && willBeRenamed) { // Rename right after creation
      nextEventType = EventType.RENAME;
      currTime += (renameDelay < activationStamp) ? renameDelay : activationStamp;
    } if ((nextEventType == EventType.CREATE && ! willBeRenamed) || nextEventType == EventType.RENAME) { // First regular access (open or listStatus)
      if (nextEventType == EventType.RENAME)
        name = name + "R"; // update name, as file has been renamed
      nextEventType = flipCoinForNextAccess();
      currTime = activationStamp;
      numAccesses++;
    } else if (currTime <= deactivationStamp) { // Regular access (open or listStatus event)
      nextEventType = flipCoinForNextAccess();
      currTime += fileType.getNextOpenInterarrival();
      numAccesses++;
    } else { // Deletion event
      if (nextEventType == EventType.DELETE)
        throw new RuntimeException("Cannot generate an event when the deletion event has already been generated.");
      nextEventType = EventType.DELETE;
      currTime = (deletionStamp > currTime) ? deletionStamp : currTime + 1;      
    }
  }
  
  public int getNumAccesses() {
    return numAccesses;
  }
  
  public EventType getNextEventType() {
    return nextEventType;
  }
  
  public long getNextEventStamp() {
    return currTime;
  }
  
  public long getCreationStamp() {
    return creationStamp;
  }
}
