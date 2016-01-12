package test;

import static org.junit.Assert.*;

import org.junit.Test;

import com.sun.jersey.core.util.Base64;

import utility.SecurityUtility;

public class SecurityTest {

	@Test
	public void test() {
		fail("Not yet implemented");
	}
	
	@Test
	public void EncDec() throws Exception {
		String text = "Hello";
		SecurityUtility util = new SecurityUtility();
		byte[] key = util.F(SecurityUtility.K, text.getBytes());
		
		System.out.println(new String(Base64.encode(key), "utf-8"));
		byte[] enc = util.Enc(key, text.getBytes());
		String result = new String(util.Dec(key, enc));
		System.out.println(text + " >>>>>" + result);
	}
}
