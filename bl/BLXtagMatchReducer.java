package bl;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import bl.BLNode.NodeType;

import com.google.gson.Gson;

public class BLXtagMatchReducer extends Reducer<Text, Text, Text, NullWritable> {

	private BLNode root;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		root = new Gson().fromJson(context.getConfiguration()
				.get("expr"), BLNode.class);
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String id = key.toString();
		ArrayList<String> xtraps = new ArrayList<String>();
		for (Text val : values) {
			xtraps.add(val.toString());
		}
		if (evaluate_expr(root, id, xtraps))
			context.write(key, null);
	}

	private boolean evaluate_expr(BLNode node, String id,
			ArrayList<String> xtraps) {
		if (node.getType() == NodeType.Leaf) {
			return xtraps.contains(node.getValue());
		}
		if (node.getType() == NodeType.NOT) {
			return !xtraps.contains(node.getValue());
		}
		if (node.getType() == NodeType.AND) {
			for (BLNode n : node.getChildren()) {
				if (!evaluate_expr(n, id, xtraps))
					return false;
			}
			return true;
		}
		if (node.getType() == NodeType.OR) {
			for (BLNode n : node.getChildren()) {
				if (evaluate_expr(n, id, xtraps))
					return true;
			}
		}
		return false;
	}
}
