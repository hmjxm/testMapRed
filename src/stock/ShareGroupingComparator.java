package stock;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ShareGroupingComparator extends WritableComparator
{
	public ShareGroupingComparator()
	{
		super(SharePair.class, true);
	}
	
	public int compare(WritableComparable wc1, WritableComparable wc2) {
		SharePair pair = (SharePair) wc1;
		 SharePair pair2 = (SharePair) wc2;
		 return pair.getCode().compareTo(pair2.getCode());
		 }
}
