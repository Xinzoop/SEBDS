package rng;

import java.io.IOException;

import ope.OPEModel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import com.google.gson.Gson;
import com.sun.jersey.core.util.Base64;

import utility.SecurityUtility;

public class RNGSearchMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	protected OPEModel model;
	protected double left;
	private String bl;
	private int li;
	private int ri;
	private String br;
	protected double right;
	protected SecurityUtility util = new SecurityUtility();
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		String json = context.getConfiguration().get("m");
		model = new Gson().fromJson(json, OPEModel.class);
		String l = context.getConfiguration().get("l");
		String r = context.getConfiguration().get("r");
		int i = l.indexOf(":");
		left = Double.parseDouble(l.substring(0, i));
		bl = l.substring(i+1);
		i = r.indexOf(":");
		right = Double.parseDouble(r.substring(0, i));
		br = r.substring(i+1);
		li = model.cIndex(left);
		ri = model.cIndex(right);
	}

	@Override
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = ivalue.toString();
		int index = line.indexOf('\t');
		if (index < 0)
			return;

		int bIndex = line.indexOf(':');
		if (bIndex < 0)
			return;

		double val = Double.parseDouble(line.substring(0, bIndex));

		try {
			int pIndex = model.cIndex(val);
			if(pIndex < li || pIndex > ri)
				return;
			
			String cb = line.substring(bIndex + 1, index);
			String b = new String(util.Dec(SecurityUtility.K,
					Base64.decode(cb.getBytes())));
			
			if(pIndex == li){
				if(b.equals(bl) && val < left)
					return;
				if(bl.equals("0") && b.equals("1"))
					return;
			}
				
			if(pIndex == ri){
				if(b.equals(bl) && val > right)
					return;
				if(br.equals("1") && b.equals("0"))
					return;
			}
			
			long p = model.decRng(val, b);
			
			String cextra = line.substring(index + 1);
			String pextra = new String(util.Dec(SecurityUtility.K,
					Base64.decode(cextra.getBytes())));
			context.write(new LongWritable(p), new Text(pextra));
		} catch (Exception e) {
			// TODO: handle exception
			System.err.println(StringUtils.stringifyException(e));
		}
	}
}
