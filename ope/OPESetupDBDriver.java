package ope;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.gson.Gson;

import utility.DummyReducer;
import utility.Util;

public class OPESetupDBDriver {
	
	public static void main(String[] args) throws Exception {
		
		if (args.length < 2){
			System.out.println("Require two arguments: 'input' and 'work directory'.");
			return;
		}
		Configuration conf = new Configuration();
		Util util = new Util(args[1]);
		util.clearEnv(conf, true);
		util.clearModel(conf);
		
		Job jobWordScan = Job.getInstance(conf, "Range Scan");
		jobWordScan.setJarByClass(OPESetupDBDriver.class);
		// TODO: specify a mapper
		jobWordScan.setMapperClass(OPERangeMapper.class);
		// TODO: specify a reducer
		jobWordScan.setReducerClass(OPERangeReducer.class);

		// TODO: specify output types
		jobWordScan.setMapOutputValueClass(LongWritable.class);
		jobWordScan.setOutputKeyClass(Text.class);
		jobWordScan.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(jobWordScan, new Path(util.ROOT + "/" + args[0]));
		// Word-file output
		FileOutputFormat.setOutputPath(jobWordScan, new Path(util.ROOT + "/" + util.getWordDir()));
		if (!jobWordScan.waitForCompletion(true))
			return;
		
		long max = jobWordScan.getCounters().findCounter(OPERangeReducer.Range.max).getValue();
		long min = jobWordScan.getCounters().findCounter(OPERangeReducer.Range.min).getValue();
		
		OPEModel model = new OPEModel();
		long dis = (max - min) / 10;
		int randDis = 100;
		if(dis > Integer.MAX_VALUE)
			randDis = Integer.MAX_VALUE;
		else if(dis < Integer.MIN_VALUE)
			randDis = Integer.MIN_VALUE;
		else
			randDis = (int)dis;
		model.split(min, max, randDis);
		String modelStr = new Gson().toJson(model);
		util.saveModel(conf, modelStr);
		
		Configuration confEnc = new Configuration();
		confEnc.set("m", modelStr);
		
		Job jobSetupDB = Job.getInstance(confEnc, "Setup Database");
		jobSetupDB.setJarByClass(OPESetupDBDriver.class);
		// TODO: specify a mapper
		jobSetupDB.setMapperClass(OPEEncMapper.class);
		// TODO: specify a reducer
		jobSetupDB.setNumReduceTasks(0);

		// TODO: specify output types
		jobSetupDB.setMapOutputValueClass(NullWritable.class);
		jobSetupDB.setOutputKeyClass(Text.class);
		jobSetupDB.setOutputValueClass(NullWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(jobSetupDB, new Path(util.ROOT + "/" + args[0]));
		// EDB output
		FileOutputFormat.setOutputPath(jobSetupDB, new Path(util.ROOT + "/" + util.getEDBDir()));
		if (!jobSetupDB.waitForCompletion(true))
			return;
		
	}
}
