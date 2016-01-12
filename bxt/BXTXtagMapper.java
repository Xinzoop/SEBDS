package bxt;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.sun.jersey.core.util.Base64;

import utility.SecurityUtility;

public class BXTXtagMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private SecurityUtility util = new SecurityUtility();
	
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		
		String[] segments = ivalue.toString().split("\t");
		if (segments.length < 2)
			return;
		
		String w = segments[0];
		try {

			byte[] Xtrap = util.F(SecurityUtility.K, w.getBytes());
			for (int c = 1; c < segments.length; ++c) {
				String id = segments[c];
				// Partition - TBD
				byte[] Xtag = util.F(Xtrap, id.getBytes());
				context.write(new Text(Base64.encode(Xtag)), null);
			}
		} catch (Exception ex) {
			System.out.println("BXTEncryptionMapper: " + ex.getMessage());
		}
	}
}
