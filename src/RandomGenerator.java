package org.apache.hadoop.fs.nnmetadata;

public interface RandomGenerator<T> {
  public T next();
  public T expectedValue(T span);
  public T[] next(long n, T span);
}
