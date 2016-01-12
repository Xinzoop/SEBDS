package sks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.jersey.core.util.Base64;

import utility.DummyReducer;
import utility.SecurityUtility;
import utility.Util;

public class SKSSearchDriver {

	public static void main(String[] args) throws Exception {
		
		if (args.length < 2){
			System.out.println("Require two arguments: 'Term' and 'work directory path'.");
			return;
		}
		
		String term = args[1];
		SecurityUtility su = new SecurityUtility();
		byte[] K1 = su.F(SecurityUtility.K, ("1" + term).getBytes());
		byte[] K2 = su.F(SecurityUtility.K, ("2" + term).getBytes());
		
		Configuration conf = new Configuration();
		conf.set("K1", new String(Base64.encode(K1)));
		conf.set("K2", new String(Base64.encode(K2)));
		
		Util util = new Util(args[0]);
		util.clearEnv(conf, false);
		
		Job job = Job.getInstance(conf, "Search");
		job.setJarByClass(sks.SKSSearchDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(SKSSearchMapper.class);
		
		job.setNumReduceTasks(0);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path(util.ROOT + "/" + util.getEDBDir()));
		FileOutputFormat.setOutputPath(job, new Path(util.ROOT + "/" + util.getOutDir()));

		if (!job.waitForCompletion(true))
			return;
		
		System.out.println(util.readFiles(util.getOutDir(), conf));
	}

}
