package bl;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.sun.jersey.core.util.Base64;

import utility.SecurityUtility;

public class BLSearchMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	private String[] stags;
	private byte[][] K1;
	private byte[][] K2;
	SecurityUtility util = new SecurityUtility();
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		stags = context.getConfiguration().get("stags").split("\\t");
		K1 = new byte[stags.length][];
		K2 = new byte[stags.length][];
		int i=0;
		for(String stag : stags){
			String[] keys = stag.split(":");
			K1[i] = Base64.decode(keys[0]);
			K2[i] = Base64.decode(keys[1]);
			++i;
		}
	}

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		
		String line = ivalue.toString();
		int index = line.indexOf('\t');
		if (index < 0)
			return;
		
		String l = line.substring(0, index);
		String d = line.substring(index + 1);
		
		int i=0;
		do{
			try
			{
				String mc = new String(util.Dec(K2[i], Base64.decode(d)));
				index = mc.indexOf(':');
				if (index < 0)
					continue;
				
				String m = mc.substring(0, index);
				String c = mc.substring(index + 1);
				String el = new String(Base64.encode(util.F(K1[i], c.getBytes())));
				if (el.equals(l)){
					context.write(new Text(m), null);
					return;
				}
			}
			catch (Exception ex){
				//System.out.println("SCHSearchMapper: " + ex.getMessage());
			}
		} while(i++<stags.length);
	}

}
