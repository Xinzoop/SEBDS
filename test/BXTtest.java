package test;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;


public class BXTtest {

	@Test
	public void test() {
		String test = "Hello World";
		System.out.println(new String(test.getBytes()));
	}

	@Test
	public void testReadFile(){
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(new URI("hdfs://master:54310"), conf);
			String content = "";
			String line;
			for(FileStatus s :  fs.listStatus(new Path("/bxt/input"))){
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(s.getPath())));
				while ((line=br.readLine()) != null) {
					content += ":" + line;
				}	
			}
			if(content.length() > 0)
				content = content.substring(1);
			System.out.println(content);
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e.getMessage());
		}
	}
	
	@Test
	public void reader(){
	      try {
	    	  String content = "";
	    	  String p = "hdfs://master:54310/input/file01";
	    	  BufferedReader fis = new BufferedReader(new FileReader(p));
	          String pattern = null;
	          while ((pattern = fis.readLine()) != null) {
	        	  content += pattern;
	          }
	          System.out.println(content);
	        } catch (IOException ioe) {
	          System.err.println("Caught exception while parsing the cached file '"
	              + StringUtils.stringifyException(ioe));
	        }
	}
}
