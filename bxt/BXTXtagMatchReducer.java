package bxt;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BXTXtagMatchReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
	
	private int xcount;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		xcount = Integer.valueOf(context.getConfiguration().get("xcount"));
	}

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		int sum = 0;
		for(IntWritable val : values){
			sum += val.get();
		}
		
		if(sum == xcount){
			context.write(key, null);
		}
	}
}
