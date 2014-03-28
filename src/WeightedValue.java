package org.apache.hadoop.fs.nnmetadata;


public final class WeightedValue<T> implements Comparable<WeightedValue<T>> {
  private T key;
  private double value;
  
  public WeightedValue(T k, double v)
  {
    this.key = k;
    this.value = v;
  }

  public int compareTo(WeightedValue<T> that)
  {
    return Double.compare(this.value, that.value);
  }
  
  public T getKey()
  {
    return key;
  }
  
  public double getValue()
  {
    return value;
  }
}

