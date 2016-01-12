package bl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BLNode implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public enum NodeType{
		Leaf,
		AND,
		OR,
		NOT
	}
	private String value;
	private NodeType type;
	private ArrayList<BLNode> children;
	private int cost;
	
	public BLNode() {
		// TODO Auto-generated constructor stub
		value = "";
		type = NodeType.Leaf;
		cost = 1;
		children = new ArrayList<>();
	}
	public static BLNode newLeaf(String value){
		return new BLNode(value, NodeType.Leaf);
	}
	public static BLNode newBranch(NodeType type){
		return new BLNode("", type);
	}
	public BLNode(String value, NodeType type) {
		this();
		this.value = value;
		this.type = type;
		this.cost = 1;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public NodeType getType() {
		return type;
	}

	public void setType(NodeType type) {
		this.type = type;
	}

	public ArrayList<BLNode> getChildren() {
		return children;
	}
	public BLNode add(BLNode child){
		children.add(child);
		return this;
	}
	
	public BLNode insert(int index, BLNode child){
		children.add(index, child);
		return this;
	}
	public int getCost() {
		return cost;
	}
	public void setCost(int cost) {
		this.cost = cost;
	}
	
	private static final Pattern pLeaf = Pattern.compile("^\\s*([^\\((\\))\\|\\&\\!]+)(.*)$");
	private static final Pattern pBracket = Pattern.compile("^\\s*\\((.*)$");
	private static final Pattern pAnd = Pattern.compile("^\\s*\\&(.*)$");
	private static final Pattern pOr = Pattern.compile("^\\s*\\|(.*)$");
	private static final Pattern pNot = Pattern.compile("^\\s*\\!(.*)$");
	
	private static class ExprUtil{
		public String expr;
	}
	
	private static int findRB(String expr){
		int index = 0;
		int counter = 0;
		for(char c : expr.toCharArray()){
			if(c == '(')
				++counter;
			if(c == ')'){
				if(counter == 0)
					return index;
				--counter;
			}
			++index;
		}
		return 0;
	}
	
	public static BLNode trans(String expr){
		ExprUtil util = new ExprUtil();
		BLNode node = readNode(expr, util);
		Matcher m;
		if ((m = pAnd.matcher(util.expr)).find()){
			BLNode nAnd = BLNode.newBranch(NodeType.AND);
			nAnd.add(node);
			while(((m = pAnd.matcher(util.expr)).find())){
				nAnd.add(readNode(m.group(1), util));
			}
			return nAnd;
		}
		else if ((m = pOr.matcher(util.expr)).find()){
			BLNode nOr = BLNode.newBranch(NodeType.OR);
			nOr.add(node);
			while(((m = pOr.matcher(util.expr)).find())){
				nOr.add(readNode(m.group(1), util));
			}
			return nOr;
		} 
		return node;
	}
	
	private static BLNode readNode(String expr, ExprUtil util){
		Matcher m;
		if((m = pLeaf.matcher(expr)).find()){
			util.expr = m.group(2);
			return BLNode.newLeaf(m.group(1));
		}
		else if((m = pBracket.matcher(expr)).find()){
			int index = findRB(m.group(1));
			util.expr = m.group(1).substring(index + 1);
			return trans(m.group(1).substring(0, index));
		}
		else if((m = pNot.matcher(expr)).find()){
			BLNode node = BLNode.newBranch(NodeType.NOT);
			node.add(readNode(m.group(1), util));
			return node;
		}
		return null;
	}
}
