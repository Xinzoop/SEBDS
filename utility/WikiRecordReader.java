package utility;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class WikiRecordReader extends RecordReader<Text, Text> {

	private Path[] paths;
	private int i;
	private Text key;
	private Text value;
	private Configuration conf;
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return i / paths.length;
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		CombineFileSplit split = (CombineFileSplit)arg0;
		paths = split.getPaths();
		conf = arg1.getConfiguration();
		i=0;
		
		System.out.println(paths.length);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(i >= paths.length)
			return false;
		
		Path p = paths[i++];
		key = new Text(p.getName());
		FileSystem fs = p.getFileSystem(conf);
		String content = "";
		String line;
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
		while ((line=br.readLine()) != null) {
			content += "\n" + line;
		}
		br.close();
		value = new Text(content);
		return true;
	}

}
