package stock;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ShareMain
{

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"hmj_stock");
		
		job.setMapperClass(ShareMapper.class);//设置处理类
		job.setMapOutputKeyClass(SharePair.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setReducerClass(ShareReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setGroupingComparatorClass(ShareGroupingComparator.class);
		job.setJarByClass(ShareMain.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.out.println(job.waitForCompletion(true));
	}
	
}
