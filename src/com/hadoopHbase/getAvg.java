package com.hadoopHbase;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.hadoopHbase.CompositeKey;
import com.hadoopHbase.NaturalValue;
import com.hadoopHbase.CompositKeyComparator;
import com.hadoopHbase.NaturalKeyGroupingComparator;

public class getAvg {
	public static void main(String[] args) {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "10.175.201.205:2181");
		conf.set("hbase.rootdir", "hdfs://10.175.201.205:8020/hbase");
		conf.set("hbase.master", "10.175.201.205:60000");
//		conf.set(TableInputFormat.INPUT_TABLE,"hsl");
		
		try{
			Job job = Job.getInstance(conf,"scanTable");
			job.setJarByClass(getAvg.class);
			
			job.setMapOutputKeyClass(CompositeKey.class);
			job.setMapOutputValueClass(NaturalValue.class);
			
			job.setReducerClass(stockReduce.class);
			job.setOutputValueClass(Text.class);

			 job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
			 job.setSortComparatorClass(CompositKeyComparator.class);
			
			Scan scan = new Scan();
			scan.setCaching(500); 
			scan.setCacheBlocks(false); 
			scan.addFamily(Bytes.toBytes("stockInfo"));
			TableMapReduceUtil.initTableMapperJob("JYQstock3", scan, Mapper.class,CompositeKey.class, NaturalValue.class, job);	
			FileOutputFormat.setOutputPath(job, new Path("hdfs://10.175.201.205:8020/usr/jinyaqin/hbaseout"));
			
			System.exit(job.waitForCompletion(true)?0:1);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static class Mapper extends TableMapper< CompositeKey, NaturalValue>{
		private final CompositeKey reducerKey = new CompositeKey();
		private final NaturalValue reducerValue =new NaturalValue();
        public void map(ImmutableBytesWritable key,Result values,Context context )
			throws IOException, InterruptedException{
        	
			String code = new String(values.getRow()).split("_")[0];
        	String date1 = new String(values.getRow()).split("_")[1]; //日期
        	KeyValue closePrice = values.getColumnLatest("stockInfo".getBytes(), "price".getBytes());
        	String format= date1; //日期
			 SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");  
			 Date date;
			try {
				date = sdf.parse(format);
				long stockTimeStamp =date.getTime(); //日期时间戳
				 reducerKey.set(code,stockTimeStamp); //key的pair-<股票代码，时间戳>
				 //将价格转化为double类型
				 double price = Double.parseDouble(new String(closePrice.getValue()));
				 reducerValue.set(stockTimeStamp, price); //value的pair-<时间戳，价格>				
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			 context.write(reducerKey,reducerValue );
			 
        }
	}
	
	//reduce
		public static class stockReduce extends Reducer<CompositeKey,NaturalValue,Text,Text>{
			@Override
			public void reduce(CompositeKey key ,Iterable<NaturalValue> values,Context context)
					throws IOException ,InterruptedException{
					StringBuilder builder = new StringBuilder();
					int i=0; //循环到3天就取平均数
					int k=0;
					int len=0;
					//Iterable只能遍历一次
					List<NaturalValue> matrixAs = new ArrayList<NaturalValue>();
					//第一次遍历取长度
					
					for(NaturalValue e : values){
						NaturalValue m = e.clone(e);
						matrixAs.add(m); //将值赋给另外一个list,再遍历就会执行,按顺序添加
						len++ ; //总长度
					}
					/*matrixAs.clear();*/
					double avgPrice=0; //平均价格
					double sumPrice=0;
					DecimalFormat  df = new DecimalFormat("######0.00");   //设置价格保留两位小数
					int quotient = (int)len/3;//商
					int remainder = len%3;
					builder.append("(");
					for(NaturalValue  data : matrixAs){
						i++;
						k++;
						sumPrice += data.getPrice();
						if(i==3){
							avgPrice=sumPrice/3;
							df.format(avgPrice);
							builder.append(avgPrice);
							builder.append(",");
							i=0; 
							sumPrice=0;//将总和置为0
							avgPrice=0;//将平均数置为0
						}
						else{ //不为3的时候判断是不是倒数第二条和倒数第三条数据，
							
							if(k==len-1 && remainder!=0){
								builder.append(data.getPrice());
								builder.append(",");
							}
							else if(k==len && remainder!=0){
								builder.append(data.getPrice());
								builder.append(")");
							}
						}

						/*String StringDate;//日期
						try {
							StringDate = data.getDate();
							builder.append(StringDate);
							double price =data.getPrice();
							builder.append(",");
							builder.append(price);
							builder.append(")");
							
						} catch (DateParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}	
						}
					context.write(new Text(key.getStockCode()),new Text(builder.toString()));*/
						
					}
					context.write(new Text(key.getStockCode()),new Text(builder.toString()));
			}
		}
		
}
