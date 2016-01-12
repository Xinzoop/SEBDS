package sks;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EDBWordMapper extends Mapper<Text, Text, Text, Text> {
	
	@Override
	protected void setup(Context context) 
			throws IOException, InterruptedException {
	}
	
	public void map(Text ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		
		StringTokenizer itr = new StringTokenizer(ivalue.toString());
	      while (itr.hasMoreTokens()) {
	        context.write(new Text(itr.nextToken()), ikey);
	      }
	}

}
