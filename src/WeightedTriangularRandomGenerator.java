package org.apache.hadoop.fs.nnmetadata;

import java.util.Random;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.commons.lang.ArrayUtils;

public class WeightedTriangularRandomGenerator implements RandomGenerator<Long> {
	private Random random;
	private ArrayList<WeightedValue<Long>> weights;
	private boolean acceptExactMatches = true; // default was false; why?
	private int numTrials = 1000;
	
	/**
	 * @param rnd	The random number generator
	 * @param keys	Array of size s of keys
	 * @param weights	Array of size s of weights, one for each key
	 */
	public WeightedTriangularRandomGenerator(Random rnd, long[] keys, double[] weights)
	{
		this.random = rnd;
		this.weights = new ArrayList<WeightedValue<Long>>();

		double sum = 0;
		double acum = 0;
		
		if (keys.length != weights.length)
		{
			throw new IllegalArgumentException("Mismatch key/weights length: " + keys.length + " vs " + weights.length);
		}
		
		for (double w : weights)
			sum += w;

		for (int i = 0; i < keys.length; i++)
		{
			acum += weights[i];
			this.weights.add(new WeightedValue<Long>(new Long(keys[i]), acum/sum));
		}
	}
	
	public WeightedTriangularRandomGenerator(Random rnd, long[] keys, double[] weights, boolean acceptEM)
	{
		this(rnd, keys, weights);
		this.acceptExactMatches = acceptEM;
	}

	private double triangular(double u, double a, double b, double c)
	{
		//double U = rand() / (double) Random.RAND_MAX;
		//double u = Math.random();
		
		double f = (c - a) / (b - a);
		
		if (u <= f)
			return a + Math.sqrt(u * (b - a) * (c - a));
		else
			return b - Math.sqrt((1 - u) * (b - a) * (b - c));
	}

	private boolean areConsecutive(Long k1, Long k2)
	{
		long diff = k1.longValue() - k2.longValue();
		if (diff == 1 || diff == -1)
			return true;
		/*
		if (k1 instanceof Integer && k2 instanceof Integer)
		{
			int diff = ((Integer) k1).intValue() - ((Integer) k2).intValue();
			if (diff == 1 || diff == -1)
				return true;
		}
		
		if (k1 instanceof Short && k2 instanceof Short)
		{
			int diff = ((Short) k1).shortValue() - ((Short) k2).shortValue();
			if (diff == 1 || diff == -1)
				return true;
		}*/
		
		return false;
	}
	
	@Override
	public Long next() {
		if (this.weights.size() == 0)
			throw new UnsupportedOperationException("Empty weights list.");
		if (this.weights.size() == 1)
			return this.weights.get(0).getKey();
		
		double ind = random.nextDouble();
		int cual = Collections.binarySearch(this.weights, new WeightedValue<Long>(null, ind));
		
		if (cual > 0 && acceptExactMatches) //exact match
			return this.weights.get(cual).getKey();
		cual = -cual - 1; // Collections.binarySearch returns: index of the search key, if it is contained in the list; otherwise, (-(insertion point) - 1)
		if (cual < 1)
			return this.weights.get(cual).getKey();
		if (areConsecutive(this.weights.get(cual).getKey(),this.weights.get(cual - 1).getKey()))
			return this.weights.get(cual).getKey();
		else
			return new Long(Math.round( triangular(ind, this.weights.get(cual - 1).getKey(),
							this.weights.get(cual).getKey(),
							(this.weights.get(cual - 1).getValue() > this.weights.get(cual).getValue()) ? this.weights.get(cual - 1).getKey() : this.weights.get(cual).getKey()) ));
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

