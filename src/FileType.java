package org.apache.hadoop.fs.nnmetadata;

/**
 * A type of file with distinct statistical access patterns.
 */
public class FileType {
  protected int name = -1;
  protected RandomGenerator<Long> AOD = null;
  protected RandomGenerator<Long> interarrivals = null;
  protected RandomGenerator<Long> activation = null;
  protected RandomGenerator<Long> activeSpan = null;
  protected RandomGenerator<Long> createsGen = null;
  protected RandomGenerator<Long> perFileOpenGen = null;

  public FileType(int t, RandomGenerator<Long> aod, RandomGenerator<Long> activation, RandomGenerator<Long> deactivation,
      RandomGenerator<Long> creates, RandomGenerator<Long> perFileOpenInterarrivals)
  {
    this.name = t;
    this.AOD = aod;
    this.activation = activation;
    this.activeSpan = deactivation;
    this.createsGen = creates;
    this.perFileOpenGen = perFileOpenInterarrivals;
  }
  
  public int getName()
  {
    return this.name;
  }
  
  public long getNextSpan()
  {
    return AOD.next();
  }
  
  public long getNextCreateInterarrival()
  {
    return createsGen.next();
  }
  
  public long getNextOpenInterarrival()
  {
    return this.perFileOpenGen.next();
  }
  
 public long getNextActivationStamp()
 {
    return activation.next();
 }
 
 public long getNextActiveSpan()
 {
   return activeSpan.next();
 }
}
