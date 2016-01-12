package utility;

import java.security.MessageDigest;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

public class SecurityUtility {
	
	public static byte[] K = "monashmonashmona".getBytes();
	
	private static final String AES_KEY_ALGORITHM = "AES";
	private static final String AES_ENCRYPT_ALGORITHM = "AES/ECB/PKCS5Padding";
	
	/*
	 * Hash
	 */
	public byte[] F(byte[] key, byte[] text) 
			throws Exception {
		MessageDigest digest = MessageDigest.getInstance("MD5");
		digest.update(key);
		return digest.digest(text);
	}
	

	/*
	 * Symmetric encryption
	 */
	public byte[] Enc(byte[] key, byte[] text) 
			throws Exception {
		Cipher cipher = Cipher.getInstance(AES_ENCRYPT_ALGORITHM);
		SecretKeySpec spec = new SecretKeySpec(key, AES_KEY_ALGORITHM);
		cipher.init(Cipher.ENCRYPT_MODE, spec);
		return cipher.doFinal(text);
	}
	
	/*
	 * Symmetric decryption
	 */
	public byte[] Dec(byte[] key, byte[] text) 
			throws Exception {
		Cipher cipher = Cipher.getInstance(AES_ENCRYPT_ALGORITHM);
		SecretKeySpec spec = new SecretKeySpec(key, AES_KEY_ALGORITHM);
		cipher.init(Cipher.DECRYPT_MODE, spec);
		return cipher.doFinal(text);
	}
}
