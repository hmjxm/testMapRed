package com.hadoopHbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.commons.httpclient.util.DateParseException;
import org.apache.commons.httpclient.util.DateUtil;
import org.apache.hadoop.io.Writable;

public class NaturalValue 
	implements Writable, Comparable<NaturalValue>{
	
	private long stockTimeStamp;
	private double price;
	
/*	public static NaturalValue copy(NaturalValue value){
		return new NaturalValue(value.stockTimeStamp,value.price);
	}*/
	
	public NaturalValue(long stockTimeStamp ,double price){
		set(stockTimeStamp,price);
	}
	
	public NaturalValue() {
	}
	
	public NaturalValue(NaturalValue e) {
		// TODO Auto-generated constructor stub
		
	}

	public void set(long timestamp, double price) {
		this.stockTimeStamp = timestamp;
		this.price = price;
	}	
	
	public long getStockTimestamp() {
		return this.stockTimeStamp;
	}
	
	public double getPrice() {
		return this.price;
	}


	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.stockTimeStamp=in.readLong();
		this.price=in.readDouble();
	}
	
	public static NaturalValue read(DataInput in) throws IOException {
		NaturalValue value = new NaturalValue();
		value.readFields(in);
		return value;
	}

	public String getDate() throws DateParseException, ParseException {
		//将时间戳转化为日期格式
		SimpleDateFormat format =  new SimpleDateFormat("yyyy-MM-dd");  
	    String d = format.format(this.stockTimeStamp);  
	 	return d;
		
	}
	
	public NaturalValue clone(NaturalValue e) {
	       return new NaturalValue(e.getStockTimestamp(), e.getPrice());
	    }

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(this.stockTimeStamp);
		out.writeDouble(this.price);
	}
	
	public int compareTo(NaturalValue data) {
		if (this.stockTimeStamp  < data.stockTimeStamp ) {
			return -1;
		} 
		else if (this.stockTimeStamp  > data.stockTimeStamp ) {
			return 1;
		}
		else {
		   return 0;
		}
	}
	

}
