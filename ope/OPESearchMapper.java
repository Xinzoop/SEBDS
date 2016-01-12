package ope;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import utility.SecurityUtility;
import com.google.gson.Gson;
import com.sun.jersey.core.util.Base64;

public class OPESearchMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	protected OPEModel model;
	protected double left;
	protected double right;
	protected SecurityUtility util = new SecurityUtility();
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		super.setup(context);
		String json = context.getConfiguration().get("m");
		model = new Gson().fromJson(json, OPEModel.class);
		left = context.getConfiguration().getDouble("l", 0);
		right = context.getConfiguration().getDouble("r", 0);
	}
	
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		String line = ivalue.toString();
		int index = line.indexOf('\t');
		if(index  < 0)
			return;
		
		double val = Double.parseDouble(line.substring(0, index));
		if(val > left && val < right){
			try {
				long p = model.dec(val);
				String cextra = line.substring(index + 1);
				String pextra = new String(util.Dec(SecurityUtility.K, Base64.decode(cextra.getBytes())));
				context.write(new LongWritable(p), new Text(pextra));	
			} catch (Exception e) {
				// TODO: handle exception
				System.err.println(StringUtils.stringifyException(e));
			}
		}
	}	
}