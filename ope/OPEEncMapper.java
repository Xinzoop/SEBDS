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

public class OPEEncMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	protected OPEModel model;
	protected Random rand;
	protected SecurityUtility util = new SecurityUtility();
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		super.setup(context);
		String json = context.getConfiguration().get("m");
		model = new Gson().fromJson(json, OPEModel.class);
		rand = new Random(System.currentTimeMillis());
	}
	
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		String line = ivalue.toString();
		int index = line.indexOf('\t');
		if(index  < 0)
			return;
		try {
			long val = Long.parseLong(line.substring(0, index));
			String extra = line.substring(index + 1);
			String cextra = new String(Base64.encode(util.Enc(SecurityUtility.K, extra.getBytes())));
			context.write(new Text(enc(val) + "\t" + cextra), null);
		} catch (Exception e) {
			// TODO: handle exception
			System.err.println(e.getMessage());
		}
	}
	
	protected String enc(long val){
		try {
			int i = model.pIndex(val);
			OPEModel.Range p = model.getPlainRanges().get(i);
			OPEModel.Range c = model.getCypherRanges().get(i);
			double scale = (double)(c.right - c.left) / (p.right - p.left);
			double noise = (rand.nextDouble() - 0.5) * scale;
			
			double cval = c.left + (val - p.left) * scale + noise;
		
			return "" + cval;
			
		} catch (Exception e) {
			// TODO: handle exception
			System.err.println("OPEEncMapper: " + StringUtils.stringifyException(e));
			return "";
		}
	}
}