package com.hadoopHbase;

import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<CompositeKey, NaturalValue> {

	@Override
	public int getPartition(CompositeKey key, NaturalValue value,int numberOfPartitions) {
		return Math.abs((int) (hash(key.getStockCode()) % numberOfPartitions));
	}
	
 /**
  *  adapted from String.hashCode()
  */
 static long hash(String str) {
    long h = 1125899906842597L; // prime
    int length = str.length();
    for (int i = 0; i < length; i++) {
       h = 31*h + str.charAt(i);
    }
    return h;
 }	
}
