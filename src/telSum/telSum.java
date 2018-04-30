package telSum;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class telSum {
	
	public static class TelSumMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
		{
			String[] record = value.toString().split(",");
			try {
				context.write(new Text(record[0]), new LongWritable(Long.parseLong(record[1])));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static class TelSumReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

		protected void reduce(Text key, Iterable<LongWritable> values,Context context)
		{
			long sum = 0;
			Iterator<LongWritable> it = values.iterator();
			while(it.hasNext()){
				sum += it.next().get();
			}
			try {
				context.write(key, new LongWritable(sum));
			} catch (Exception e) {
				e.printStackTrace();//抛出write函数的异常
			}
		}
		
	}

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try{
			Job job = Job.getInstance(conf,"hmjTelSum");
			
			job.setMapperClass(TelSumMapper.class);//设置处理类
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(LongWritable.class);
			
			job.setReducerClass(TelSumReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			
			job.setJarByClass(telSum.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			System.out.println(job.waitForCompletion(true));
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}

}
