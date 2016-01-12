package utility;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;

public class WikiCombineInputFormat extends CombineFileInputFormat<Text, Text> {

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException {
		// TODO Auto-generated method stub
		return new WikiRecordReader();
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		// TODO Auto-generated method stub
		return false;
	}
}
