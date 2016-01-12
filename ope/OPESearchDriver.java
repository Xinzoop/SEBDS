package ope;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.gson.Gson;
import utility.Util;

public class OPESearchDriver extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new OPESearchDriver(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length < 2){
			System.out.println("Require two arguments:'work directory' and 'search range'.");
			return 1;
		}
		
		long st = System.currentTimeMillis();
		
		Configuration conf = getConf();
		Util util = new Util(args[0]);
		util.clearEnv(conf, false);
		
		conf.setLong(FileInputFormat.SPLIT_MAXSIZE, 16 * 1024 * 1024);
		
		// Read Model
		String modelStr = util.readModel(conf);
		OPEModel model = new Gson().fromJson(modelStr, OPEModel.class);
		
		int left = Integer.parseInt(args[1]);
		int right = left;
		if(args.length > 2)
			right = Integer.parseInt(args[2]);
		
		double l = model.encWithoutNoise(left, true);
		double r = model.encWithoutNoise(right, false);
		conf.set("m", modelStr);
		conf.setDouble("l", l);
		conf.setDouble("r", r);
		
		Job job = Job.getInstance(conf, "Search");
		job.setJarByClass(OPESearchDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(OPESearchMapper.class);
		
		job.setNumReduceTasks(0);

		// TODO: specify output types
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path(util.ROOT + "/" + util.getEDBDir()));
		FileOutputFormat.setOutputPath(job, new Path(util.ROOT + "/" + util.getOutDir()));

		if (!job.waitForCompletion(true))
			return 1;
		
		System.out.println("88888888888888888888888888888888888888888888888888888888888888888888888");
		String result = util.readFiles(util.getOutDir(), conf);
		if(result.isEmpty()){
			System.out.println("Oops, there are no matched files found.");
		}
		else{
			System.out.println("Search Results:");
			System.out.println(result);	
		}
		System.out.println("Elapsed Time (sec): " + Math.round((System.currentTimeMillis() - st) / 1000));
		System.out.println("88888888888888888888888888888888888888888888888888888888888888888888888");
		return 0;
	}

}
