package bxt;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.sun.jersey.core.util.Base64;

import utility.SecurityUtility;

public class BXTXtagMatchMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private String[] arrayXtrap;
	private String[] arrayDoc;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		arrayXtrap = context.getConfiguration().get("xtraps").split(":");
		arrayDoc = context.getConfiguration().get("doctemp").split("\n");
	}
	
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		
		String xtag = ivalue.toString().replaceAll("\\s+", "");
		
		try {
			SecurityUtility util = new SecurityUtility();
			for(String id : arrayDoc){
				for(String xtrap : arrayXtrap){
					String tag = new String(Base64.encode(util.F(Base64.decode(xtrap), id.getBytes())));
					if(tag.compareTo(xtag) == 0){
						context.write(new Text(id), one);
						return;
					}
				}
			}
			
		} catch (Exception e) {
			// TODO: handle exception
			System.err.println(e.getMessage());
		}
	}

}
