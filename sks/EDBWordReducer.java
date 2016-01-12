package sks;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EDBWordReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		ArrayList<String> fNameList = new ArrayList<>();
		// process values
		for (Text val : values) {
			int i=0;
			boolean flag = true;
			for (;i<fNameList.size(); ++i){
				int comp = fNameList.get(i).compareTo(val.toString());
				if (comp < 0)
					continue;
				if (comp == 0)
					flag = false;
				break;
			}
			
			if (flag)
				fNameList.add(i, val.toString());
		}
		
		StringBuilder sb = new StringBuilder();
		for (String val : fNameList){
			sb.append("\t" + val);
		}
		context.write(key, new Text(sb.substring(1)));
	}
}
