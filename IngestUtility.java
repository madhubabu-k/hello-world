/**
 * 
 */
package com.pearson.ps.ingest.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.documentum.fc.client.IDfDocument;
import com.documentum.fc.client.IDfFolder;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import com.pearson.ps.ingest.main.IngestConstants;

/**
 * @author Manav Leslie
 *
 */
public final class IngestUtility {
	
	/**
	 * Private constructor to prevent creating instances
	 */
	private IngestUtility() {
	}


	private static final Logger LOGGER = LoggerFactory.getLogger(IngestUtility.class);
	
	/**
	 * Utility method to make a HTTP get call to the specified URL 
	 * @param strAsURL URL 
	 * @return success or failure 
	 */
	public static boolean connectAsHTTPGet(String strAsURL) {
		LOGGER.info("connect() - start to {}" ,strAsURL);

		try {
			HttpClient client = new DefaultHttpClient();
			HttpGet httpGet = new HttpGet(strAsURL);
			HttpResponse response = client.execute(httpGet);
			BufferedReader rd = new BufferedReader(new InputStreamReader(
					response.getEntity().getContent()));
			int statusCode = response.getStatusLine().getStatusCode();

			LOGGER.info("connect() - htttResponseCode : {}" , statusCode);
			if (statusCode != HttpStatus.SC_OK) {
				LOGGER.error("Method failed: {}" , response.getStatusLine());
				return false;
			}
			String line = "";
			while ((line = rd.readLine()) != null) {
				LOGGER.debug(line);
				
			}
		} catch (IOException e) {
			LOGGER.error(e.getMessage(), e);
			return false;
		} catch (Exception ex) {
			LOGGER.error(ex.getMessage(), ex);
			return false;
		}
		LOGGER.info("connect() - end");
		return true;
	}
	
	
	/**
	 * Returns current date in the specified format  
	 * @param datePattern  Date Format 
	 * @return String 
	 */
	public static String getDateAsFormattedString(String datePattern ) {
		
		Calendar calendar = Calendar.getInstance();
		SimpleDateFormat dateFormat = new SimpleDateFormat(datePattern);
		return  dateFormat.format(calendar.getTime());
	}
	
	/**
	 * Returns a formatted string with right pad space added between the strings 
	 * @param leftStr Left String 
	 * @param rightStr Right String 
	 * @param padLength padding length 
	 * @return Modified String 
	 */
	public static String getRightPaddedString (String leftStr, String rightStr, int padLength ) {
		return  StringUtils.rightPad(leftStr, padLength) + rightStr;
	}
	
	
	/**
	 * Method to get The BU Name for the country specific BU.
	 * 
	 * @param userBU
	 * @return
	 */
	public static String getBUKey(String userBU) {

		String strBUName = "";

		if (userBU.toUpperCase().startsWith("DK")) {
			strBUName = "DK";
		} else if (userBU.toUpperCase().startsWith("PI")) {
			strBUName = "PI";
		} else if (userBU.toUpperCase().startsWith("PENGUIN")) {
			strBUName = "Penguin";
		}

		return strBUName;
	}	
	
	/**
	 * Method to truncate the attribute value according to its expected length.
	 * 
	 * @param attrValue
	 * @param attrLength
	 * @return
	 */
	public static String truncate(String attrValue, int attrLength) {

		if (attrValue == null || "".equals(attrValue)) {
			return attrValue;
		}
		String trucString = "";
		try {
			if (attrValue.getBytes("UTF-8").length <= attrLength
					|| attrLength == 0) {
				return attrValue;
			}

			trucString = attrValue
					.substring(0, attrLength
							- (attrValue.getBytes("UTF-8").length - attrValue
									.length()));

		} catch (UnsupportedEncodingException e) {
			LOGGER.info(e.getMessage());
		}

		return trucString;

	}
	
	/**
	 * This Method returns Product ID of the documents and folders
	 * 
	 * @param strObjectId
	 * @param loSession
	 * @return
	 */

	public static String getProductId(String strObjectId, IDfSession loSession) {

		String strlsProductId = "";
		String strfldrTypeName = "";
		String folderID = strObjectId;

		try {

			if (strObjectId.startsWith("09")) {

				IDfDocument lodocObj = (IDfDocument) loSession
						.getObject(new DfId(strObjectId));
				folderID = lodocObj.getRepeatingString("i_folder_id", 0);
			}

			IDfFolder lofldrObj = (IDfFolder) loSession.getObject(new DfId(
					folderID));

			int prdCount = lofldrObj.getValueCount("i_ancestor_id");

			for (int i = 0; i < prdCount; i++) {

				String foldersID = lofldrObj.getRepeatingString(
						"i_ancestor_id", i);
				IDfFolder lofldrObj1 = (IDfFolder) loSession
						.getObject(new DfId(foldersID));
				strfldrTypeName = lofldrObj1.getTypeName();

				if (strfldrTypeName
						.equalsIgnoreCase(IngestConstants.PRSN_PS_DK_FOLDER)
						|| strfldrTypeName
								.equalsIgnoreCase(IngestConstants.PRSN_PS_PI_FOLDER)
						|| strfldrTypeName
								.equalsIgnoreCase(IngestConstants.PRSN_PS_PENGUIN_FOLDER)) {

					strlsProductId = foldersID;
					break;
				}
			}

			if (strlsProductId.equals("")) {
				strlsProductId = lofldrObj.getRepeatingString("i_ancestor_id",
						prdCount - 1);
			}

		} catch (DfException dfe) {
			LOGGER.error(dfe.getLocalizedMessage(), dfe);

		}
		return strlsProductId;
	}
	
	
}