package rng;

import org.apache.hadoop.util.StringUtils;

import com.sun.jersey.core.util.Base64;

import ope.OPEEncMapper;
import ope.OPEModel;
import utility.SecurityUtility;

public class RNGEncMapper extends OPEEncMapper {

	@Override
	protected String enc(long val) {
		// TODO Auto-generated method stub
		try {
			int i = model.pIndex(val);
			OPEModel.Range p = model.getPlainRanges().get(i);
			long cl = model.getCypherRanges().get(i).left;
			if(i > 0)
				cl = model.getCypherRanges().get(i-1).left;
			long cr = model.getCypherRanges().get(i).right;
			double scale = (double)(cr - cl) / (p.right - p.left);
			double noise = (rand.nextDouble() - 0.5) * scale;
			
			if(val == 11){
				System.out.println("i:" + i + ",cl-cr:" + cl + "-" + cr + ",scale:" + scale + ",noise:" + noise);
			}
			
			double cval = cl + (val - p.left) * scale + noise;
			String b = "1";
			if (i > 0 && model.getCypherRanges().get(i-1).contains(cval)){
				b = "0";
			}
			
			String cb = new String(Base64.encode(util.Enc(SecurityUtility.K, b.getBytes())));
			return "" + cval + ":" + cb;
			
		} catch (Exception e) {
			// TODO: handle exception
			System.err.println("OPEEncMapper: " + StringUtils.stringifyException(e));
			return "";
		}
	}
}
