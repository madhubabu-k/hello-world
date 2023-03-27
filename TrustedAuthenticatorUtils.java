package com.pearson.ps.ingest.utils;

/**
 * Copyright (C) 2011 Pearson Plc. All Rights Reserved $Workfile:
 * TrustedAuthenticatorUtils.java $
 * 
 * @description This is the utility class for decrypting the encrypted password
 *              and vice versa of a trusted authenticator for a docbase.
 ********************************************************************************************** 
 *              Change Log Date Description ==== ===========
 * 
 *              06 Jul'11 Created
 * 
 *              
 *              *****************************************************************
 *              ******************************
 */

public final class TrustedAuthenticatorUtils {
	/**
	 * Private constructor to prevent creating instances
	 */
	private TrustedAuthenticatorUtils() {
	}

	/**
	 * Encrypt the trusted authenticator password using a caesar cipher.
	 * 
	 * @param str
	 * @return String
	 */
	public static final String encrypt(String str) {
		String tempStr = null;
		if (str != null) {
			char ac[] = (com.pearson.ps.ingest.utils.TrustedAuthenticatorUtils.class)
					.getName().toCharArray();
			char ac1[] = str.toCharArray();
			StringBuffer stringbuffer = new StringBuffer(ac1.length * 2);
			int i = 0;
			int j = ac.length - 1;
			for (; i < ac1.length; i++) {
				stringbuffer.append(Integer.toHexString(ac1[i] + ac[j]));
				j = ((ac.length + j) - 1) % ac.length;
			}
			tempStr = stringbuffer.toString();
		}
		return tempStr;
	}

	/**
	 * Decrypt the trusted authenticator password using a caesar cipher.
	 * 
	 * @param str
	 * @return String
	 */
	public static final String decrypt(String str) {
		String tempStr = null;
		if (str != null) {
			char ac[] = (com.pearson.ps.ingest.utils.TrustedAuthenticatorUtils.class)
					.getName().toCharArray();
			char ac1[] = str.toCharArray();
			StringBuffer stringbuffer = new StringBuffer(ac1.length / 2);
			int i = 0;
			int j = ac.length - 1;
			for (; i < ac1.length; i += 2) {
				char c = (char) Integer.parseInt(new String(ac1, i, 2), 16);
				stringbuffer.append((char) (c - ac[j]));
				j = ((ac.length + j) - 1) % ac.length;
			}
			tempStr = stringbuffer.toString();
		}
		return tempStr;
	}
}
