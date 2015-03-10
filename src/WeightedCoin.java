package org.apache.hadoop.fs.nnmetadata;

import java.util.Random;

public final class WeightedCoin
{
  private double weight;
  private Random random;

  public WeightedCoin(double w, Random r)
  {
    this.weight = w;
    this.random = r;
  }

  public boolean flip()
  {
    return this.random.nextDouble() < this.weight;
  }
}
