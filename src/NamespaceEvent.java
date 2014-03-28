package org.apache.hadoop.fs.nnmetadata;


public class NamespaceEvent {
  private String filename;
  private String baseDir;
  MyFile.EventType eventType;
  private long timestamp;
  
  public NamespaceEvent(MyFile f)
  {
    filename = f.getName();
    baseDir = f.getBaseDir();
    timestamp = f.getNextEventStamp();
    eventType = f.getNextEventType();
  }
  
  public String getFilename() {
    return baseDir + filename;
  }
  
  public MyFile.EventType getEventType() {
    return eventType;
  }
  
  public long getTimestamp() {
    return timestamp;
  }
}
