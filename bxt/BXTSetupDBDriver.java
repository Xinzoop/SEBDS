package bxt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import sks.EDBWordMapper;
import sks.EDBWordReducer;
import sks.SKSEncryptionMapper;
import utility.DummyReducer;
import utility.Util;
import utility.WikiCombineInputFormat;

public class BXTSetupDBDriver {

	public static void main(String[] args) throws Exception {
		
		if (args.length < 2){
			System.out.println("Require two arguments: 'input' and 'work directory'.");
			return;
		}
		Configuration conf = new Configuration();
		Util util = new Util(args[1]);
		util.clearEnv(conf, true);
		
		// 1st: Word Scan
		Job jobWordScan = Job.getInstance(conf, "Word Scan");
		jobWordScan.setJarByClass(BXTSetupDBDriver.class);
		
		jobWordScan.setInputFormatClass(WikiCombineInputFormat.class);
		
		// TODO: specify a mapper
		jobWordScan.setMapperClass(EDBWordMapper.class);
		// TODO: specify a reducer
		jobWordScan.setReducerClass(EDBWordReducer.class);

		// TODO: specify output types
		jobWordScan.setOutputKeyClass(Text.class);
		jobWordScan.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(jobWordScan, new Path(util.ROOT + "/" + args[0]));
		
		// Word-file output
		FileOutputFormat.setOutputPath(jobWordScan, new Path(util.ROOT + "/" + util.getWordDir()));
		if (!jobWordScan.waitForCompletion(true))
			return;
		
		// 2cd: Setup EDB
		Job jobSetupDB = Job.getInstance(conf, "Setup Database");
		jobSetupDB.setJarByClass(BXTSetupDBDriver.class);
		// TODO: specify a mapper
		jobSetupDB.setMapperClass(SKSEncryptionMapper.class);
		// TODO: specify a reducer
		jobSetupDB.setNumReduceTasks(0);

		// TODO: specify output types
		jobSetupDB.setOutputKeyClass(Text.class);
		jobSetupDB.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(jobSetupDB, new Path(util.ROOT + "/" + util.getWordDir()));
		// EDB output
		FileOutputFormat.setOutputPath(jobSetupDB, new Path(util.ROOT + "/" + util.getEDBDir()));
		if (!jobSetupDB.waitForCompletion(true))
			return;
		
		//3rd: Setup XSet
		Job jobSetupXSet = Job.getInstance(conf, "Setup XSet");
		jobSetupXSet.setJarByClass(BXTSetupDBDriver.class);
		// TODO: specify a mapper
		jobSetupXSet.setMapperClass(BXTXtagMapper.class);
		// TODO: specify a reducer
		jobSetupXSet.setNumReduceTasks(0);

		// TODO: specify output types
		jobSetupXSet.setOutputKeyClass(Text.class);
		jobSetupXSet.setOutputValueClass(NullWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(jobSetupXSet, new Path(util.ROOT + "/" + util.getWordDir()));
		// EDB output
		FileOutputFormat.setOutputPath(jobSetupXSet, new Path(util.ROOT + "/" + util.getXsetDir()));
		if (!jobSetupXSet.waitForCompletion(true))
			return;
	}
}
