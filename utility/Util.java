package utility;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Util {
	public final static String ROOT = "hdfs://master:54310";
	
	private String dir;
	
	public Util(String dir){
		this.dir = dir;
	}
	
	public String getWordDir(){
		return dir + "/word-file";
	}
	public String getEDBDir(){
		return dir + "/edb";
	}
	public String getTempDir(){
		return dir + "/temp";
	}
	public String getXsetDir(){
		return dir + "/xset";
	}
	public String getOutDir(){
		return dir + "/out";
	}
	
	public String getModelDir(){
		return dir + "/m";
	}
	
	public void clearEnv(Configuration conf, boolean setup){
		try {
			FileSystem fs = FileSystem.get(new URI(ROOT), conf);
			if(setup){
				if(fs.exists(new Path("/" + getWordDir()))){
					fs.delete(new Path("/" + getWordDir()), true);
				}
				if(fs.exists(new Path("/" + getEDBDir()))){
					fs.delete(new Path("/" + getEDBDir()), true);
				}
				if(fs.exists(new Path("/" + getXsetDir()))){
					fs.delete(new Path("/" + getXsetDir()), true);
				}
			}
			else{
				if(fs.exists(new Path("/" + getOutDir()))){
					fs.delete(new Path("/" + getOutDir()), true);
				}
				if(fs.exists(new Path("/" + getTempDir()))){
					fs.delete(new Path("/" + getTempDir()), true);
				}
			}
			fs.close();
			
		} catch (Exception e) {
			// TODO: handle exception
			System.err.println(e.getMessage());
		}
	}
	
	public void clearModel(Configuration conf){
		try {
			FileSystem fs = FileSystem.get(new URI(ROOT), conf);
			if(fs.exists(new Path("/" + getModelDir()))){
				fs.delete(new Path("/" + getModelDir()), true);
			}
			fs.close();
		} catch (Exception e) {
			// TODO: handle exception
			System.err.println(e.getMessage());
		}
	}

	public String readModel(Configuration conf) {
		try {
			FileSystem fs = FileSystem.get(new URI(ROOT), conf);
			String content = "";
			String line;
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(new Path("/" + getModelDir()))));
			while ((line = br.readLine()) != null) {
				content += line.replaceAll("\\s+", "");
			}
			fs.close();
			return content;
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e.getMessage());
			return "";
		}
	}
	
	public void saveModel(Configuration conf, String modelStr) {
		try {
			FileSystem fs = FileSystem.get(new URI(ROOT), conf);
			OutputStream os = fs.create(new Path("/" + getModelDir()));
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
			br.write(modelStr);
			br.close();
			fs.close();
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e.getMessage());
		}
	}
	
	public String readFiles(String path, Configuration conf) {
		try {
			FileSystem fs = FileSystem.get(new URI(ROOT), conf);
			String content = "";
			String line;
			for(FileStatus s :  fs.listStatus(new Path("/" + path))){
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(s.getPath())));
				while ((line=br.readLine()) != null) {
					content += "\n" + line;
				}	
			}
			fs.close();
			if(content.length() > 0)
				content = content.substring(1);
			return content;
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e.getMessage());
			return "";
		}
	}
}
