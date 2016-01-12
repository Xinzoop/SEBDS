package ope;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class OPERangeMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	private long max;
	private long min;
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		max = Long.MIN_VALUE;
		min = Long.MAX_VALUE;
	}
	
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		String line = ivalue.toString();
		int index = line.indexOf('\t');
		if(index  < 0)
			return;
		
		long val = Long.parseLong(line.substring(0, index));
		
		if(val > max)
			max = val;
		if(val < min)
			min = val;
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		context.write(new Text(OPERangeReducer.Range.max.toString()), new LongWritable(max));
		context.write(new Text(OPERangeReducer.Range.min.toString()), new LongWritable(min));
	}
}