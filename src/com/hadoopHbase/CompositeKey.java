package com.hadoopHbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.*;

//自定义pair
public class CompositeKey implements WritableComparable<CompositeKey>{
	private String stockCode;
	private long stockTimeStamp;
	
	public CompositeKey(String stockCode, long stockTimeStamp){
		set(stockCode, stockTimeStamp);
	}
	
	public void set(String stockCode, long stockTimeStamp) {
		// TODO Auto-generated method stub
		this.stockCode=stockCode;
		this.stockTimeStamp=stockTimeStamp;
	}

	public CompositeKey(){
		
	}

	
	public String getStockCode(){
		return this.stockCode;
	}
	
	public long getStockTimeStamp(){
		return this.stockTimeStamp;
	}
	
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.stockCode=in.readUTF(); //反序列化
		this.stockTimeStamp=in.readLong();
	}
	
	public void write(DataOutput out) throws IOException{
		out.writeUTF(this.stockCode);//序列化
		out.writeLong(this.stockTimeStamp);
	}
	
	public int compareTo(CompositeKey pair){
		if(this.stockCode.compareTo(pair.stockCode) != 0){
			return this.stockCode.compareTo(pair.stockCode);
		}
		else if(this.stockTimeStamp != pair.stockTimeStamp){
			return stockTimeStamp < pair.stockTimeStamp ? -1:1;
		}
		else 
			return 0;
	}
	
	
	public static class CompositeKeyComparator extends WritableComparator {
		public CompositeKeyComparator() {
			super(CompositeKey.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}
	}

	static { // register this comparator
		WritableComparator.define(CompositeKey.class,
				new CompositeKeyComparator());
	}

}
