/**
 * 
 */
package com.pearson.ps.ingest.utils;

import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.documentum.com.DfClientX;
import com.documentum.com.IDfClientX;
import com.documentum.fc.client.DfQuery;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.client.IDfValidator;
import com.documentum.fc.client.IDfValueAssistance;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.IDfAttr;
import com.documentum.fc.common.IDfList;
import com.documentum.operations.IDfFormatRecognizer;
import com.pearson.ps.ingest.exception.IngestException;

/**
 * @author Manav Leslie
 * 
 */
public final class DctmUtility {

	/**
	 * Private constructor to prevent creating instances
	 */
	private DctmUtility() {
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(DctmUtility.class);

	public static boolean isDctmObjectPresent(IDfSession session,
			String strTargetPath) {

		LOGGER.info("Checking for SysObject : {}", strTargetPath);
		boolean isDctmObjectPresent = false;

		try {
			IDfSysObject dctmSysObj = (IDfSysObject) session
					.getObjectByPath(strTargetPath);

			if (null != dctmSysObj) {
				isDctmObjectPresent = true;
			}

		} catch (DfException dfe) {
			LOGGER.error(dfe.getLocalizedMessage(), dfe);
		}

		return isDctmObjectPresent;
	}

	public static String getDataTypeAsString(int iDataType) {
		String strDataType = "";
		switch (iDataType) {
		case IDfAttr.DM_STRING:
			strDataType = "STRING";
			break;
		case IDfAttr.DM_TIME:
			strDataType = "TIME";
			break;
		case IDfAttr.DM_BOOLEAN:
			strDataType = "BOOLEAN";
			break;
		case IDfAttr.DM_INTEGER:
			strDataType = "INTEGER";
			break;
		case IDfAttr.DM_DOUBLE:
			strDataType = "DOUBLE";
			break;
		case IDfAttr.DM_ID:
			strDataType = "ID";
			break;
		default:
			break;
		}
		return strDataType;
	}

	public static boolean isObjectValueValid(IDfSysObject sysObject,
			String attrName, String attrValue) throws DfException,
			IngestException {

		boolean isValid = false;
		try {
			IDfValidator v = sysObject.getValidator();
			IDfValueAssistance va = v.getValueAssistance(attrName, null);
			if (va != null) {
				IDfList validValue = va.getActualValues();
				int currValueIndex = validValue.findStringIndex(attrValue);
				if (currValueIndex > -1) {
					isValid = true;
				}
			} else {
				// no value assistance on attribute.
				isValid = true;
			}
		} catch (DfException dfe) {
			throw new IngestException(dfe.getMessage(), dfe);
		}
		return isValid;
	}

	/*
	 * public static Map<String, String> queryFormat(IDfSession session) throws
	 * IngestException {
	 * 
	 * IDfCollection col = null;
	 * 
	 * Map<String, String> extensionMap = new HashMap<String, String>();
	 * 
	 * try { StringBuilder strQuery = new StringBuilder(); strQuery.append(
	 * "SELECT name , dos_extension FROM dm_format  where default_storage = '0000000000000000'"
	 * );
	 * 
	 * IDfQuery query = new DfQuery(); query.setDQL(strQuery.toString()); col =
	 * query.execute(session, IDfQuery.DF_QUERY);
	 * 
	 * while (col.next()) { String extension = col.getString("dos_extension");
	 * if (extension != null && !extensionMap.containsKey(extension)) {
	 * //Putting a "." infront to avoid substring for every asset
	 * extensionMap.put("."+extension, col.getString("name")); } } } catch
	 * (DfException dfe) { LOGGER.error(dfe.getLocalizedMessage(), dfe); throw
	 * new IngestException(dfe); } finally { if (col != null) { try {
	 * col.close(); } catch (DfException e) { LOGGER.error(e.getMessage());
	 * 
	 * } } } return extensionMap; }
	 */

	/**
	 * Method to get the Suggested File Format of a file.
	 */
	public static String getFileFormat(IDfSession session, String filePath) {
		String format = "";

		try {
			IDfClientX clientX = new DfClientX();
			IDfFormatRecognizer fileFormatRecog = clientX.getFormatRecognizer(
					session, filePath, null);
			if (null != fileFormatRecog) {
				format = fileFormatRecog.getDefaultSuggestedFileFormat();
			}
		} catch (DfException dfe) {
			LOGGER.error(dfe.getLocalizedMessage(), dfe);
			format = "binary";
		} catch (Exception ex) {
			LOGGER.error(ex.getLocalizedMessage(), ex);
			format = "binary";
		} finally {
			// In case the Exception could not be catch for Special Files, make
			// the format 'binary'.
			if (null == format) {
				format = "binary";
			}
		}

		return format;
	}

	public static String getFolderContentsSize(IDfSession session,
			String folderPath) throws IngestException {

		IDfCollection col = null;
		String folderSize = "";

		try {
			StringBuilder strQuery = new StringBuilder();
			strQuery.append(
					"select sum(r_content_size)/1024/1024 as foldersize from dm_document where folder('")
					.append(StringEscapeUtils.escapeSql(folderPath))
					.append("',DESCEND)");

			IDfQuery query = new DfQuery();
			query.setDQL(strQuery.toString());
			col = query.execute(session, IDfQuery.DF_QUERY);

			if (col.next()) {
				folderSize = col.getString("foldersize");
			}
		} catch (DfException dfe) {
			LOGGER.error(dfe.getLocalizedMessage(), dfe);
			throw new IngestException(dfe);
		} finally {
			if (col != null) {
				try {
					col.close();
				} catch (DfException e) {
					LOGGER.error(e.getMessage());

				}
			}
		}
		return folderSize;
	}

	public static IDfCollection execSelectQuery(String queryString,
			IDfSession session) throws DfException {
		IDfCollection col = null; // Collection for the result
		IDfClientX clientx = new DfClientX();
		IDfQuery q = clientx.getQuery();

		q.setDQL(queryString);
		col = q.execute(session, IDfQuery.DF_EXEC_QUERY);
		return col;
	}

	/**
	 * Purpose:To execute query
	 * 
	 * @param queryString
	 * @param session
	 * @return
	 * @throws DfException
	 */
	public static void execUpdateQuery(String queryString, IDfSession session)
			throws DfException {
		IDfCollection col = null; // Collection for the result
		IDfClientX clientx = new DfClientX();
		IDfQuery q = clientx.getQuery();

		try {
			q.setDQL(queryString);
			col = q.execute(session, IDfQuery.DF_EXEC_QUERY);
		} catch (DfException dfe) {
			LOGGER.error(dfe.getMessage(), dfe);
		} finally {
			if (col != null) {
				try {
					col.close();
				} catch (DfException e) {
					LOGGER.error(e.getMessage());

				}
			}
		}

	}

}
