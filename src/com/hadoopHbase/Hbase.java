package com.hadoopHbase;

import java.io.IOException;  
import java.util.StringTokenizer;  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.client.Put;  
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;  
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;  
import org.apache.hadoop.hbase.mapreduce.TableReducer;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.NullWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

@SuppressWarnings("unused")
public class Hbase {
	private static Text outKey= new Text();
	private static Text outValue = new Text();

	public static class Hmapper extends Mapper<LongWritable,Text,Text,Text>{
		@Override
		protected void map(LongWritable key ,Text value, Context context) throws IOException, InterruptedException{
			String[] splited = value.toString().split(",");
			long temp=Long.MIN_VALUE;
			long[] datestamp;
			String[] values;
			
			//在全部map之前一次执行
		/*	protected void setup(Context context2){
				
			}*/
			
			outKey.set(splited[0]+"_"+splited[1]);
			outValue.set(splited[0]+"\t"+splited[1]+"\t"+splited[2]);
			context.write(outKey, outValue);
		}
	}
	
	public static class HReducer extends TableReducer<Text,Text,NullWritable>{
		@SuppressWarnings("deprecation")
		@Override
		protected void reduce(Text k2,Iterable<Text> v2s, Context context) throws IOException , InterruptedException{
			Put put = new Put(k2.getBytes()); //设置rowkey
			for(Text v2 : v2s){
				
				String[] splits = v2.toString().split("\t");
				put.add("stockInfo".getBytes(),"price".getBytes(),splits[2].getBytes());
				
			}
			context.write(NullWritable.get(),put);
		}
	}
	
	public static void main (String[] args) throws Exception{
		Configuration conf = HBaseConfiguration.create();
		 //设置zookeeper
		conf.set("hbase.zookeeper.quorum", "10.175.201.204,10.175.201.205,10.175.201.206");
		conf.set("hbase.rootdir", "hdfs://10.175.201.205:8020/hbase");
		//设置hbase表名称
        conf.set(TableOutputFormat.OUTPUT_TABLE, "JYQstock3");
        Job job =Job.getInstance(conf,"JYQhbase");
        
        job.setMapperClass(Hmapper.class);
        job.setReducerClass(HReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        
        FileInputFormat.setInputPaths(job,"hdfs://10.175.201.205:8020/data/shares/closedata.txt");
       /* FileOutputFormat.setOutputPath(job, new Path(args[1]));*/
        /*job.setInputFormatClass(TextInputFormat.class);*/
       
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
	}

}
