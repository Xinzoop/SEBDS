package sks;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.sun.jersey.core.util.Base64;

import utility.SecurityUtility;

public class SKSSearchMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	SecurityUtility util = new SecurityUtility();
	
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		
		String line = ivalue.toString();
		int index = line.indexOf('\t');
		if (index < 0)
			return;
		
		String l = line.substring(0, index);
		String d = line.substring(index + 1);
		
		byte[] K1 = Base64.decode(context.getConfiguration().get("K1"));
		byte[] K2 = Base64.decode(context.getConfiguration().get("K2"));
		if (null == K1 || null == K2)
			return;
		
		try
		{
			String mc = new String(util.Dec(K2, Base64.decode(d)));
			index = mc.indexOf(':');
			if (index < 0)
				return;
			
			String m = mc.substring(0, index);
			String c = mc.substring(index + 1);
			String el = new String(Base64.encode(util.F(K1, c.getBytes())));
			if (el.equals(l))
				context.write(new Text(m), null);
		}
		catch (Exception ex){
			//System.out.println("SCHSearchMapper: " + ex.getMessage());
		}
	}

}
