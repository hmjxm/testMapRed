package stock;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class SharePair implements WritableComparable<SharePair>
{
	
	private Text code=new Text();
	private LongWritable date=new LongWritable();
	

	public Text getCode()
	{
		return code;
	}

	public void setCode(Text code)
	{
		this.code = code;
	}

	public LongWritable getDate()
	{
		return date;
	}

	public void setDate(LongWritable date)
	{
		this.date = date;
	}

	public void readFields(DataInput arg0) throws IOException
	{
		// TODO Auto-generated method stub
		code.readFields(arg0);
		date.readFields(arg0);
		
	}

	public void write(DataOutput arg0) throws IOException
	{
		// TODO Auto-generated method stub
		code.write(arg0);
		date.write(arg0);
	}

	public int compareTo(SharePair arg0)
	{
		// TODO Auto-generated method stub
		int compare =this.code.compareTo(arg0.getCode());
		if(compare==0)
		{
			compare=date.compareTo(arg0.getDate());
		}
		return compare;
	}
}