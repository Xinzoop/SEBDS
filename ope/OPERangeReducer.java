package ope;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OPERangeReducer extends Reducer<Text, LongWritable, Text, Text> {
	
	public enum Range {max, min}
	
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		
		String type = key.toString();
		if(type.compareTo(Range.min.toString()) == 0){
			long min = Long.MAX_VALUE;
			for(LongWritable i : values){
				if(i.get() < min)
					min = i.get();
			}
			context.getCounter(Range.min).setValue(min);
		}
		else{
			long max = Long.MIN_VALUE;
			for(LongWritable i : values){
				if(i.get() > max)
					max = i.get();
			}
			context.getCounter(Range.max).setValue(max);
		}
	}
}
