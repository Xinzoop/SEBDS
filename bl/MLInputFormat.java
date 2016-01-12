package bl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MLInputFormat extends FileInputFormat<LongWritable, MapWritable> {

	@Override
	public RecordReader<LongWritable, MapWritable> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new MLRecordReader();
	}
	
	private class MLRecordReader extends RecordReader<LongWritable, MapWritable>{

		private LongWritable key;
		private MapWritable value;
		private boolean flag;
		private BufferedReader br;
		
		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			br.close();
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return key;
		}

		@Override
		public MapWritable getCurrentValue() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return flag ? 0 : 1;
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			
			// TODO Auto-generated method stub
			
			Path p = ((FileSplit)split).getPath();
			FileSystem fs = p.getFileSystem(context.getConfiguration());
			br = new BufferedReader(new InputStreamReader(fs.open(p)));
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			key = new LongWritable(0);
			value = new MapWritable();
			String line;
			int index = 0;
			while ((line=br.readLine()) != null && index++ < 1000000) {
				value.put(new Text(line), NullWritable.get());
			}
			if(value.size() <= 0)
				return false;
			return true;
		}
	}
}
