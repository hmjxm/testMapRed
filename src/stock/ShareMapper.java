package stock;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ShareMapper extends Mapper<Object, Text, SharePair, DoubleWritable>
{

	protected void map(Object key, Text value,Context context) throws IOException, InterruptedException
	{
		String[] record = value.toString().split(",");
		SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd");
		Date date = null;
	    try {
			date = dateFormat.parse(record[1]);
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		SharePair sharePair=new SharePair();
		sharePair.setCode(new Text(record[0]));
		sharePair.setDate(new LongWritable(date.getTime()));
		context.write(sharePair, new DoubleWritable(Double.parseDouble(record[2])));
	}
	
}
