package sks;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.sun.jersey.core.util.Base64;

import utility.SecurityUtility;

public class SKSEncryptionMapper extends Mapper<LongWritable, Text, Text, Text> {

	SecurityUtility util = new SecurityUtility();
	
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		
		String[] segments = ivalue.toString().split("\t");
		if (segments.length < 2)
			return;
		
		String w = segments[0];
		try {

			byte[] K1 = util.F(SecurityUtility.K, ("1" + w).getBytes());
			byte[] K2 = util.F(SecurityUtility.K, ("2" + w).getBytes());

			for (int c = 1; c < segments.length; ++c) {
				String id = segments[c];
				byte[] l = util.F(K1, ("" + c).getBytes());
				byte[] dc = util.Enc(K2, (id + ":" + c).getBytes());
				context.write(new Text(Base64.encode(l)), new Text(Base64.encode(dc)));
			}
		} catch (Exception ex) {
			System.out.println("SKSEncryptionMapper: " + ex.getMessage());
		}
	}
}
