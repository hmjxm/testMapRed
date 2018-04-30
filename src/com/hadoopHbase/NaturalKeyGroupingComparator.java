package com.hadoopHbase;

import org.apache.hadoop.io.*;


public class NaturalKeyGroupingComparator extends WritableComparator{
	
	protected NaturalKeyGroupingComparator(){
		super(CompositeKey.class , true);
	}
	
	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2){
		CompositeKey sp1 = (CompositeKey) wc1;
		CompositeKey sp2 = (CompositeKey) wc2;
		return sp1.getStockCode().compareTo(sp2.getStockCode());
	}

}
