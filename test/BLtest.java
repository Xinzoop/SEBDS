package test;

import static org.junit.Assert.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import com.google.gson.Gson;

import bl.BLNode;
import bl.BLNode.NodeType;

public class BLtest {

	@Test
	public void test() {
		fail("Not yet implemented");
	}

	@Test
	public void expr(){
		String raw = "(w1|w2|w3)&(!(w4|w5))";
		BLNode root = BLNode.trans(raw);
		System.out.println(new Gson().toJson(root));
	}
}
