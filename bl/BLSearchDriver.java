package bl;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import bl.BLNode.NodeType;

import com.google.gson.Gson;
import com.sun.jersey.core.util.Base64;

import utility.SecurityUtility;
import utility.Util;

public class BLSearchDriver extends Configured implements Tool{

	private static SecurityUtility securityUtil = new SecurityUtility();
	
	public static ArrayList<BLNode> chooseTerm(BLNode node) throws Exception{
		ArrayList<BLNode> array = new ArrayList<BLNode>();
		if(node.getType() == NodeType.Leaf){
			array.add(node);
		}
		else if(node.getType() == NodeType.NOT){
			
		}
		else if(node.getType() == NodeType.AND){
			int minCost = Integer.MAX_VALUE;
			for(BLNode n : node.getChildren()){
				ArrayList<BLNode> temp = chooseTerm(n);
				int sumCost = 0;
				for(BLNode res : temp){
					sumCost += res.getCost();
				}
				if(sumCost < minCost){
					minCost = sumCost;
					array = temp;
				}
			}
		}
		else if(node.getType() == NodeType.OR){
			for(BLNode n : node.getChildren()){
				array.addAll(chooseTerm(n));
			}
		}
		return array;
	}
	
	public static String getReplaceXtraps(BLNode root) throws Exception{
		String xtrap = "";
		if(root.getType() == NodeType.Leaf){
			xtrap = new String(Base64.encode(securityUtil.F(SecurityUtility.K, root.getValue().getBytes())));
			// replace value
			root.setValue(xtrap);
		}
		else{
			for(BLNode n : root.getChildren()){
				xtrap += ":" + getReplaceXtraps(n);
			}
			xtrap = xtrap.substring(1);
		}
		return xtrap;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new BLSearchDriver(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length < 2){
			System.out.println("Require 'work directory path' and search condition.");
			return 1;
		}
		
		long st = System.currentTimeMillis();
		
		// Temporary
		BLNode root = BLNode.trans(args[1]);
		ArrayList<BLNode> choosedTerms = chooseTerm(root);
		if(choosedTerms.size() <= 0){
			System.out.println("The app cannot choose a valid term. The reason may because there is only one 'NOT' node in the query.");
			return 1;
		}
		String stags = "";
		for(BLNode node : choosedTerms){
			stags += "\t" +
			new String(Base64.encode(
					securityUtil.F(SecurityUtility.K, ("1" + node.getValue()).getBytes())))
			+ ":" +
			new String(Base64.encode(
					securityUtil.F(SecurityUtility.K, ("2" + node.getValue()).getBytes())));
		}
		stags = stags.substring(1);
		
		// 1st search
		Configuration conf = getConf();
		Util util = new Util(args[0]);
		util.clearEnv(conf, false);
		
		conf.set("stags", stags);
		conf.setLong(FileInputFormat.SPLIT_MAXSIZE, 16 * 1024 * 1024);
		
		Job job = Job.getInstance(conf, "1st Search");
		job.setJarByClass(BLSearchDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(BLSearchMapper.class);
		// TODO: specify a reducer
		job.setNumReduceTasks(0);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path(util.ROOT + "/" + util.getEDBDir()));
		FileOutputFormat.setOutputPath(job, new Path(util.ROOT + "/" + util.getTempDir()));
		if (!job.waitForCompletion(true))
			return 1;
		
		// Problems: the amount of intermediate data
		
		// Read intermediate data: need confirmation
		String docTemp = util.readFiles(util.getTempDir(), conf);
		if(docTemp.isEmpty()){
			System.out.println("Oops, there are no matched files found.");
			return 1;
		}
		System.out.print("888888888888888888888888888888888888888888888888888\n");
		System.out.print("Intermediate Size: " + docTemp.split("\n").length + "\n");
		System.out.print("888888888888888888888888888888888888888888888888888\n");
		
		// 2nd: check xtrap
		String xtraps = getReplaceXtraps(root);
		
		conf.set("xtraps", xtraps);
		conf.set("doctemp", docTemp);
		conf.set("expr", new Gson().toJson(root));
		
		Job jobX = Job.getInstance(conf, "Xtrap Match");
		jobX.setJarByClass(BLSearchDriver.class);
		// TODO: specify a mapper
		jobX.setMapperClass(BLXtagMatchMapper.class);
		// TODO: specify a reducer
		jobX.setReducerClass(BLXtagMatchReducer.class);
		
		jobX.setInputFormatClass(MLInputFormat.class);

		// TODO: specify output types
		jobX.setMapOutputValueClass(Text.class);
		jobX.setOutputKeyClass(Text.class);
		jobX.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(jobX, new Path(util.ROOT + "/" + util.getXsetDir()));
		FileOutputFormat.setOutputPath(jobX, new Path(util.ROOT + "/" + util.getOutDir()));

		if (!jobX.waitForCompletion(true))
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
