package com.hadoopHbase;

import org.apache.hadoop.io.*;

public class CompositKeyComparator extends WritableComparator {
	protected CompositKeyComparator(){
		super(CompositeKey.class, true);
	}
	
	@Override
	public int compare (WritableComparable wC1, WritableComparable wC2){
		CompositeKey sp1 = (CompositeKey) wC1;
		CompositeKey sp2 = (CompositeKey) wC2;
		
		int comparison = sp1.getStockCode().compareTo(sp2.getStockCode());
		if(comparison==0){
			//股票代码相同
			if(sp1.getStockTimeStamp() == sp2.getStockTimeStamp()){
				return 0 ;
			}
			else if(sp1.getStockTimeStamp() < sp2.getStockTimeStamp()){
				return -1;
			}
			else 
				return 1;
		}
		else {
			return comparison;
		}
	}

}
