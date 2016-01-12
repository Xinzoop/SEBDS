package bxt;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.jersey.core.util.Base64;

import sks.SKSSearchMapper;
import utility.DummyReducer;
import utility.SecurityUtility;
import utility.Util;

public class BXTSearchDriver {

	public static void main(String[] args) throws Exception {
		
		if (args.length < 2){
			System.out.println("Require at least two arguments: 'work directory path' and 'term'.");
			return;
		}

		// 1st: search first term - This should be the less-used term actually
		// Get first term
		String term = args[1];
		SecurityUtility su = new SecurityUtility();
		byte[] K1 = su.F(SecurityUtility.K, ("1" + term).getBytes());
		byte[] K2 = su.F(SecurityUtility.K, ("2" + term).getBytes());
		
		Configuration conf = new Configuration();
		Util util = new Util(args[0]);
		util.clearEnv(conf, false);
		
		conf.set("K1", new String(Base64.encode(K1)));
		conf.set("K2", new String(Base64.encode(K2)));
		
		Job job = Job.getInstance(conf, "1st Search");
		job.setJarByClass(BXTSearchDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(SKSSearchMapper.class);
		// TODO: specify a reducer
		job.setNumReduceTasks(0);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path(util.ROOT + "/" + util.getEDBDir()));
		if(args.length > 2)
			FileOutputFormat.setOutputPath(job, new Path(util.ROOT + "/" + util.getTempDir()));
		else
			FileOutputFormat.setOutputPath(job, new Path(util.ROOT + "/" + util.getOutDir()));

		if (!job.waitForCompletion(true) || args.length == 2)
			return;
		
		// Problems: the amount of intermediate data
		
		// 2nd: check xtrap
		int xcount = 0;
		String xtraps = "";
		for(int i = 2; i < args.length; ++i){
			String t = args[i];
			byte[] xtrap = su.F(SecurityUtility.K, t.getBytes());
			xtraps += ":" + new String(Base64.encode(xtrap));
			++xcount;
		}
		xtraps = xtraps.substring(1);
		
		// Read intermediate data: need confirmation
		String docTemp = util.readFiles(util.getTempDir(), conf);
		
		Configuration confX = new Configuration();
		confX.set("xtraps", xtraps);
		confX.set("doctemp", docTemp);
		confX.set("xcount", String.valueOf(xcount));
		
		Job jobX = Job.getInstance(confX, "Xtrap Match");
		jobX.setJarByClass(BXTSearchDriver.class);
		// TODO: specify a mapper
		jobX.setMapperClass(BXTXtagMatchMapper.class);
		// TODO: specify a reducer
		jobX.setReducerClass(BXTXtagMatchReducer.class);

		// TODO: specify output types
		jobX.setMapOutputValueClass(IntWritable.class);
		jobX.setOutputKeyClass(Text.class);
		jobX.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(jobX, new Path(util.ROOT + "/" + util.getXsetDir()));
		FileOutputFormat.setOutputPath(jobX, new Path(util.ROOT + "/" + util.getOutDir()));

		if (!jobX.waitForCompletion(true))
			return;
		
		System.out.println(util.readFiles(util.getOutDir(), conf));
	}
}
