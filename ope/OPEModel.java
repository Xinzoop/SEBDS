package ope;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class OPEModel implements Serializable{
	
	public class Range implements Serializable{
		public long left;
		public long right;
		
		public Range(){
			
		}
		public Range(long left, long right){
			this.left = left;
			this.right = right;
		}
		public boolean contains(double val){
			return val >= left && val < right;
		}
	}
	
	private List<Range> plainRanges;
	private List<Range> cypherRanges;
	
	public OPEModel() {

	}
		
	public List<Range> getPlainRanges() {
		return plainRanges;
	}

	public void setPlainRanges(List<Range> plainRanges) {
		this.plainRanges = plainRanges;
	}

	public List<Range> getCypherRanges() {
		return cypherRanges;
	}

	public void setCypherRanges(List<Range> cyherRanges) {
		this.cypherRanges = cyherRanges;
	}

	public void split(long min, long max, int randDis){
		if(randDis < 10)
			randDis = 10;
		if(min == max)
			max = min + randDis;
		if(min > max){
			long t = min;
			min = max;
			max = t;
		}
		
		plainRanges = new ArrayList<OPEModel.Range>();
		cypherRanges = new ArrayList<OPEModel.Range>();
		Random rand = new Random(System.currentTimeMillis());
		long pi = min - randDis;
		long ci = pi - rand.nextInt(randDis); 
		while(pi < max + randDis){
			int dis = rand.nextInt(randDis);
			Range r = new Range(pi, pi + dis);
			plainRanges.add(r);
			pi += dis;
			
			dis = dis + rand.nextInt(randDis * 10 - dis);
			Range c = new Range(ci, ci + dis);
			cypherRanges.add(c);
			ci += dis;
		}
	}
	
	public int pIndex(long val){
		int index = 0;
		for(Range r : plainRanges){
			if(r.contains(val))
				return index;
			++index;
		}
		return index;
	}
	
	public int cIndex(double val){
		int index = 0;
		for(Range r : cypherRanges){
			if(r.contains(val))
				return index;
			++index;
		}
		return index;
	}
	
	public long dec(double val){
		int i = cIndex(val);
		Range p = plainRanges.get(i);
		Range c = cypherRanges.get(i);
		double scale = (double)(c.right - c.left) / (p.right - p.left);
		return Math.round(p.left + (val - c.left) / scale);
	}
	
	public long decRng(double val, String b){
		int i = cIndex(val);
		Range p = plainRanges.get(i);
		if(b.equals("0"))
			p = plainRanges.get(i + 1);
		
		long cl = cypherRanges.get(i).left;
		long cr = cypherRanges.get(i).right;
		if (b.equals("0")){
			cr = cypherRanges.get(i + 1).right;
		}
		else if (i > 0){
			cl = cypherRanges.get(i - 1).left; 
		}
		
		double scale = (double)(cr - cl) / (p.right - p.left);
		return Math.round(p.left + (val - cl) / scale);
	}
	
	
	public double encWithoutNoise(long val, boolean ge){
		
		long min = plainRanges.get(0).left;
		long max = plainRanges.get(plainRanges.size() - 1).right - 1;
		if(val < min)
			val = min;
		if(val > max)
			val = max;
		
		int i = pIndex(val);
		Range p = plainRanges.get(i);
		Range c = cypherRanges.get(i);
		double scale = (double)(c.right - c.left) / (p.right - p.left);
		double cval = c.left + (val - p.left) * scale;
		if(ge)
			return cval - scale * 0.5;
		else
			return cval + scale * 0.5;
	}
	
	
	public String encRangeWithoutNoise(long val, boolean ge){
		
		long min = plainRanges.get(0).left;
		long max = plainRanges.get(plainRanges.size() - 1).right - 1;
		if(val < min)
			val = min;
		if(val > max)
			val = max;
		
		int i = pIndex(val);
		Range p = plainRanges.get(i);
		long cl = cypherRanges.get(i).left;
		long cr = cypherRanges.get(i).right;
		if (i > 0){
			cl = cypherRanges.get(i - 1).left;
		}
		
		double scale = (double)(cr - cl) / (p.right - p.left);
		double cval = cl + (val - p.left) * scale;
		if(ge)
			cval -= scale * 0.5;
		else
			cval += scale * 0.5;
		
		//b
		String b = "1";
		if(i > 0 && cypherRanges.get(i - 1).contains(cval))
			b = "0";
		return "" + cval + ":" + b;
	}
}
