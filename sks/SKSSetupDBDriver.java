package sks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utility.DummyReducer;
import utility.Util;
import utility.WikiCombineInputFormat;

public class SKSSetupDBDriver extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new SKSSetupDBDriver(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length < 2){
			System.out.println("Require two arguments: 'input' and 'work directory'.");
			return 1;
		}
		Configuration conf = getConf();
		Util util = new Util(args[1]);
		util.clearEnv(conf, true);
		
		Job jobWordScan = Job.getInstance(conf, "Word Scan");
		jobWordScan.setJarByClass(sks.SKSSetupDBDriver.class);
		
		jobWordScan.setInputFormatClass(WikiCombineInputFormat.class);
		
		// TODO: specify a mapper
		jobWordScan.setMapperClass(EDBWordMapper.class);
		// TODO: specify a reducer
		jobWordScan.setReducerClass(EDBWordReducer.class);

		// TODO: specify output types
		
		jobWordScan.setOutputKeyClass(Text.class);
		jobWordScan.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(jobWordScan, new Path(util.ROOT + "/" + args[0]));
		
		// Word-file output
		FileOutputFormat.setOutputPath(jobWordScan, new Path(util.ROOT + "/" + util.getWordDir()));
		if (!jobWordScan.waitForCompletion(true))
			return 1;
		
		Job jobSetupDB = Job.getInstance(conf, "Setup Database");
		jobSetupDB.setJarByClass(sks.SKSSetupDBDriver.class);
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
			return 1;
		return 0;
	}
}
