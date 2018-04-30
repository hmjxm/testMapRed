package stock;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ShareReducer extends Reducer<SharePair, DoubleWritable, Text,Text>
{
	protected void reduce(SharePair key, Iterable<DoubleWritable> values,
			Reducer<SharePair, DoubleWritable, Text, Text>.Context context)
	{
		Iterator<DoubleWritable> it = values.iterator();
		StringBuilder result = new StringBuilder();
		ArrayList<DoubleWritable> list = new ArrayList<DoubleWritable>();
		while (it.hasNext())
		{
			list.add(new DoubleWritable(it.next().get()));
		}
		DecimalFormat   df   =new   DecimalFormat("#.000"); 
		for (int i = 2; i < list.size(); i++)
		{
			double temp = (list.get(i).get() + list.get(i - 1).get() + list.get(i - 2).get()) / 3;
			result.append(df.format(temp));
			result.append(",");
		}
		try
		{
			context.write(key.getCode(), new Text(result.toString()));
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

}
