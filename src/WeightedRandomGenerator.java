package org.apache.hadoop.fs.nnmetadata;

import java.util.Random;
import java.util.TreeMap;
import java.util.NavigableMap;
import org.apache.commons.lang.ArrayUtils;

public class WeightedRandomGenerator implements RandomGenerator<Long> {
  private final NavigableMap<Double, Long> map = new TreeMap<Double, Long>();
  private final Random random;
  private double total = 0;
  private int numTrials = 1000;

  /**
   * @param rnd The random number generator
   * @param keys  Array of size s of keys
   * @param weights Array of size s of weights, one for each key
   */
  public WeightedRandomGenerator(Random rnd, Long[] keys, double[] weights)
  {
    this.random = rnd;

    if (keys.length != weights.length)
    {
      System.err.println("Mismatch key/weights length.");
      System.exit(0);
    }

    for (int i = 0; i < weights.length; i++)
    {
      this.add(weights[i], keys[i]);
    }
  }

  public WeightedRandomGenerator(Random rnd, Long[] keys, Double[] weights)
  {
    this.random = rnd;

    if (keys.length != weights.length)
    {
      System.err.println("Mismatch key/weights length.");
      System.exit(0);
    }

    for (int i = 0; i < weights.length; i++)
    {
      this.add(weights[i], keys[i]);
    }
  }

  private void add(double weight, Long result) {
    if (weight <= 0) return;
    total += weight;
    map.put(total, result);
  }

  @Override
  public Long next() {
    double value = random.nextDouble() * total;
    return map.ceilingEntry(value).getValue();
  }

  @Override
  public Long expectedValue(Long span) {

    long sum = 0;
    for (int i = 0; i < numTrials; i++)
    {
      long curr = 0;
      long arrivals = 1;
      while (curr < span)
      {
        curr += next();
        arrivals += 1;
      }
      if (curr > span)
        arrivals -= 1;
      sum += arrivals;
    }

    long heuristic = 100000;    
    if (sum / numTrials > heuristic)
    {
      int moreTrials = 1000;
      for (int i = 0; i < moreTrials; i++)
      {
        long curr = 0;
        long arrivals = 1;
        while (curr < span)
        {
          curr += next();
          arrivals += 1;
        }
        if (curr > span)
          arrivals -= 1;
        sum += arrivals;
      }
      return new Long(sum / (numTrials + moreTrials));
    }
    return new Long(sum / numTrials);
  }

  @Override
  public Long[] next(long n, Long span) {
    long[] list = new long[(int) (n * 1.1)];
    double sum = n * numTrials;
    long trials = numTrials;

    while (true)
    {
      long curr = 0;
      long arrivals = 1;
      int i = 0;
      while (curr < span && i < n)
      {
        list[i] = next();
        curr += list[i];
        arrivals += 1;
        i += 1;
      }
      if (curr > span)
        arrivals -= 1;

      sum += arrivals;
      trials += 1;
      n = (long) (sum / trials);
      double error = 0.0005 * n;
      if (arrivals >= (n - error)  && arrivals <= (n + error))
      {
        for (i = (int) arrivals; i < list.length ; i++)
          list[i] = -1L;
        return ArrayUtils.toObject(list);
      }
    }   
  }
}
