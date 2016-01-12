package bl;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import com.sun.jersey.core.util.Base64;

import utility.SecurityUtility;

public class BLXtagMatchMapper extends Mapper<LongWritable, MapWritable, Text, Text> {

	private String[] arrayXtrap;
	private String[] arrayDoc;
	SecurityUtility util = new SecurityUtility();
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		arrayXtrap = context.getConfiguration().get("xtraps").split(":");
		arrayDoc = context.getConfiguration().get("doctemp").split("\n");
	}
	
	public void map(LongWritable ikey, MapWritable ivalue, Context context)
			throws IOException, InterruptedException {
		
		try {
			for(String id : arrayDoc){
				for(String xtrap : arrayXtrap){
					String tag = new String(Base64.encode(util.F(Base64.decode(xtrap), id.getBytes())));
					if(ivalue.containsKey(new Text(tag))){
						context.write(new Text(id), new Text(xtrap));
					}
				}
			}
			
		} catch (Exception e) {
			// TODO: handle exception
			System.err.println(e.getMessage());
		}
	}
}
