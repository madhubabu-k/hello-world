/**
 * 
 */
package com.pearson.ps.ingest.creator;

import java.util.List;
import java.util.Map;

import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.client.IDfTypedObject;
import com.documentum.fc.client.impl.typeddata.Attribute;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.DfTime;
import com.pearson.ps.ingest.exception.IngestException;
import com.pearson.ps.ingest.main.IngestConstants;
import com.pearson.ps.ingest.utils.DctmUtility;
import com.pearson.ps.ingest.utils.IngestUtility;

/**
 * This abstract class is extended by the classes used for ingesting products
 * and assets
 * 
 * @author Manav Leslie
 * 
 */
public abstract class AbstractIngestCreator {

	/**
	 * Returns the index of the attribute
	 * 
	 * @param attrName
	 *            - Name of the attribute
	 * @return index of the attribute
	 */
	protected int getAttrIndex(String attrName) {

		// directly used 16 as we know the attrs "contributor_name" and
		// "contributor_role" has 16 characters.
		String attrIndex = attrName.substring(16);
		if (attrIndex.indexOf("_") != -1) {
			attrIndex = attrIndex.replace("_", "");
		}
		return Integer.parseInt(attrIndex);
	}

	/**
	 * This method is called to add system object attributes to the object.
	 * 
	 * @param session
	 *            IDfSession
	 * @param sysObject
	 *            The object to which attributes are added.
	 * @param metaInfo
	 *            Map containing metadata information
	 * @param orderedKeysList
	 *            List containing ordered Keys information
	 * @param loFormReqNoObj
	 *            Form object
	 * @throws IngestException
	 *             Custom Exception thrown by the method
	 * @throws DfException
	 */
	protected void addSysObjectAttributes(IDfSession session,
			IDfSysObject sysObject, Map<String, String> prodMetaDataProp,
			List<String> orderedKeysList, IDfTypedObject loDfTypedObj)
			throws IngestException, DfException {

		preInitDocObjForContributerAttrs(sysObject);

		for (int count = 0; count < orderedKeysList.size(); count++) {
			String key = orderedKeysList.get(count);
			String value = (String) prodMetaDataProp.get(key);
			// logger.info("key "+key +"  value  "+value);

			setSysObejctAttribute(key, value, sysObject, loDfTypedObj);
		}
	}

	/**
	 * Pre-initialise the attribute values assuming that all 5 values are blank
	 * 
	 * @param sysObject
	 *            Object on which the attributes are being set
	 * @throws IngestException
	 *             Custom Exception thrown
	 */
	protected void preInitDocObjForContributerAttrs(IDfSysObject sysObject)
			throws IngestException {

		try {
			sysObject.setRepeatingString(IngestConstants.ATTR_CONTRIBUTOR_NAME,
					0, "");
			sysObject.setRepeatingString(IngestConstants.ATTR_CONTRIBUTOR_NAME,
					1, "");
			sysObject.setRepeatingString(IngestConstants.ATTR_CONTRIBUTOR_NAME,
					2, "");
			sysObject.setRepeatingString(IngestConstants.ATTR_CONTRIBUTOR_NAME,
					3, "");
			sysObject.setRepeatingString(IngestConstants.ATTR_CONTRIBUTOR_NAME,
					4, "");
			sysObject.setRepeatingString(IngestConstants.ATTR_CONTRIBUTOR_ROLE,
					0, "");
			sysObject.setRepeatingString(IngestConstants.ATTR_CONTRIBUTOR_ROLE,
					1, "");
			sysObject.setRepeatingString(IngestConstants.ATTR_CONTRIBUTOR_ROLE,
					2, "");
			sysObject.setRepeatingString(IngestConstants.ATTR_CONTRIBUTOR_ROLE,
					3, "");
			sysObject.setRepeatingString(IngestConstants.ATTR_CONTRIBUTOR_ROLE,
					4, "");
		} catch (DfException dfe) {
			throw new IngestException(dfe.getMessage(), dfe);
		}

	}

	/**
	 * Method to set Sys object attributes
	 * 
	 * @param key
	 * @param value
	 * @param sysObject
	 * @param loFormReqNoObj
	 * @throws DfException
	 * @throws IngestException
	 */
	protected void setSysObejctAttribute(String key, String value,
			IDfSysObject sysObject, IDfTypedObject loDfTypedObj)
			throws DfException, IngestException {

		for (int j = 0; j < loDfTypedObj.getAttrCount(); j++) {

			String name = loDfTypedObj.getAttr(j).getName();

			if (name.equals(key)) {

				boolean rob = false;
				boolean hasAttribute = false;
				rob = name.equals("r_object_id");

				if (!rob) {
					hasAttribute = sysObject.hasAttr(value);
				}

				if (hasAttribute) {

					int formAttrDataType = loDfTypedObj.getAttr(j)
							.getDataType();

					int sysObjectAttrDataType = sysObject
							.getAttrDataType(value);

					int sysObjectAttrLength = sysObject.getAttr(
							sysObject.findAttrIndex(value)).getLength();

					if (formAttrDataType == sysObjectAttrDataType) {

						if (loDfTypedObj.isAttrRepeating(name)) {

							String repeatString = loDfTypedObj
									.getAllRepeatingStrings(name, ",");

							if (!sysObject.isAttrRepeating(value)) {
								if (formAttrDataType == Attribute.DM_BOOLEAN) {
									if (repeatString.equalsIgnoreCase("F"))
										sysObject.setBoolean(value, false);
									else if (repeatString.equalsIgnoreCase("T"))
										sysObject.setBoolean(value, true);
								} else if (formAttrDataType == Attribute.DM_DOUBLE) {
									sysObject.setDouble(value, Double
											.parseDouble(repeatString));
								} else if (formAttrDataType == Attribute.DM_ID) {
									sysObject.setId(value, new DfId(
											repeatString));
								} else if (formAttrDataType == Attribute.DM_INTEGER) {
									sysObject.setInt(value, Integer
											.parseInt(repeatString));
								} else if (formAttrDataType == Attribute.DM_STRING) {

									repeatString = IngestUtility.truncate(
											repeatString, sysObjectAttrLength);
									if (DctmUtility.isObjectValueValid(
											sysObject, value, repeatString)) {
										sysObject
												.setString(value, repeatString);
									}

								} else if (formAttrDataType == Attribute.DM_TIME) {

									sysObject
											.setTime(
													value,
													new DfTime(
															repeatString,
															IngestConstants.DEFAULT_TIME_FORMAT));
								}
							} else {
								String[] repeatvalues = null;
								repeatvalues = repeatString.split(",");
								for (int counter = 0; counter < repeatvalues.length; counter++) {
									String currValue = repeatvalues[counter];

									if (formAttrDataType == Attribute.DM_BOOLEAN) {
										if (currValue.equalsIgnoreCase("F"))
											sysObject.setRepeatingBoolean(
													value, counter, false);
										else if (currValue
												.equalsIgnoreCase("T"))
											sysObject.setRepeatingBoolean(
													value, counter, true);
									} else if (formAttrDataType == Attribute.DM_DOUBLE) {
										sysObject
												.setRepeatingDouble(
														value,
														counter,
														Double
																.parseDouble(currValue));
									} else if (formAttrDataType == Attribute.DM_ID) {
										sysObject.setRepeatingId(value,
												counter, new DfId(currValue));
									} else if (formAttrDataType == Attribute.DM_INTEGER) {
										sysObject.setRepeatingInt(value,
												counter, Integer
														.parseInt(currValue));
									} else if (formAttrDataType == Attribute.DM_STRING) {

										currValue = IngestUtility.truncate(
												currValue, sysObjectAttrLength);

										if (DctmUtility.isObjectValueValid(
												sysObject, value, repeatString)) {
											sysObject.setRepeatingString(value,
													counter, currValue);
										}

									} else if (formAttrDataType == Attribute.DM_TIME) {

										sysObject
												.setRepeatingTime(
														value,
														counter,
														new DfTime(
																currValue,
																IngestConstants.DEFAULT_TIME_FORMAT));
									}
								}
							}
						} else {

							String currValue = loDfTypedObj.getValueAt(j)
									.toString();

							// Check for Form's Contributor Type
							// Attributes
							// (The Mapping for these attributes is
							// different)
							if (isContributorAttr(name)) {

								int counter = getAttrIndex(name) - 1;

								currValue = IngestUtility.truncate(currValue,
										sysObjectAttrLength);

								sysObject.setRepeatingString(value, counter,
										currValue);

							} else {
								if (sysObject.isAttrRepeating(value)) {

									/*
									 * if the attr is single valued in form or
									 * in B3/EPM tables but corresponding attr
									 * in Product folder is repeating then we
									 * will spilt that value comma separated and
									 * insert each value repeatedly on folder's
									 * repeating attr.
									 */
									String[] repeatvalues = null;
									repeatvalues = currValue.split(",");

									for (int counter = 0; counter < repeatvalues.length; counter++) {
										currValue = repeatvalues[counter];

										if (formAttrDataType == Attribute.DM_BOOLEAN) {
											if (currValue.equalsIgnoreCase("F"))
												sysObject.setRepeatingBoolean(
														value, counter, false);
											else if (currValue
													.equalsIgnoreCase("T"))
												sysObject.setRepeatingBoolean(
														value, counter, true);
										} else if (formAttrDataType == Attribute.DM_DOUBLE) {
											sysObject
													.setRepeatingDouble(
															value,
															counter,
															Double
																	.parseDouble(currValue));
										} else if (formAttrDataType == Attribute.DM_ID) {
											sysObject.setRepeatingId(value,
													counter,
													new DfId(currValue));
										} else if (formAttrDataType == Attribute.DM_INTEGER) {
											sysObject
													.setRepeatingInt(
															value,
															counter,
															Integer
																	.parseInt(currValue));
										} else if (formAttrDataType == Attribute.DM_STRING) {

											currValue = IngestUtility.truncate(
													currValue,
													sysObjectAttrLength);

											if (DctmUtility
													.isObjectValueValid(
															sysObject, value,
															currValue)) {
												sysObject.setRepeatingString(
														value, counter,
														currValue);
											}

										} else if (formAttrDataType == Attribute.DM_TIME) {

											sysObject
													.setRepeatingTime(
															value,
															counter,
															new DfTime(
																	currValue,
																	IngestConstants.DEFAULT_TIME_FORMAT));
										}
									}
								} else {

									if (formAttrDataType == Attribute.DM_BOOLEAN) {
										if (currValue.equalsIgnoreCase("F"))
											sysObject.setBoolean(value, false);
										else if (currValue
												.equalsIgnoreCase("T"))
											sysObject.setBoolean(value, true);
									} else if (formAttrDataType == Attribute.DM_DOUBLE) {
										sysObject.setDouble(value, Double
												.parseDouble(currValue));
									} else if (formAttrDataType == Attribute.DM_ID) {
										sysObject.setId(value, new DfId(
												currValue));
									} else if (formAttrDataType == Attribute.DM_INTEGER) {
										sysObject.setInt(value, Integer
												.parseInt(currValue));
									} else if (formAttrDataType == Attribute.DM_STRING) {

										currValue = IngestUtility.truncate(
												currValue, sysObjectAttrLength);

										if (DctmUtility.isObjectValueValid(
												sysObject, value, currValue)) {
											sysObject.setString(value,
													currValue);
										}

									} else if (formAttrDataType == Attribute.DM_TIME) {

										sysObject
												.setTime(
														value,
														new DfTime(
																currValue,
																IngestConstants.DEFAULT_TIME_FORMAT));
									}
								}
							}
						}
					} else {
						throw new IngestException(
								"*** Error *** Data Type of Form Attribute : '"
										+ name
										+ " and Data Type of Object Attribute : "
										+ value + " for object type :: "
										+ sysObject.getTypeName()
										+ " did not match."
										+ IngestConstants.NEWLINE);
					}
				} else {
					throw new IngestException("** Error ** Attribute: " + value
							+ " does not exist for object type :: "
							+ sysObject.getTypeName() + IngestConstants.NEWLINE);
				}
				break;
			}
		}
		// sysObject.save();
	}

	/**
	 * Method to check if the attribute name is that of contributor
	 * 
	 * @param attrName
	 *            attribute name
	 * @return boolean value
	 */
	protected boolean isContributorAttr(String attrName) {
		// TODO re-look at the method. retained from old code.
		boolean contributorNameAttr = false;
		contributorNameAttr = (attrName.equals("contributor_name1")
				|| attrName.equals("contributor_name2")
				|| attrName.equals("contributor_name3")
				|| attrName.equals("contributor_name4")
				|| attrName.equals("contributor_name5")
				|| attrName.equals("contributor_name_1")
				|| attrName.equals("contributor_name_2")
				|| attrName.equals("contributor_name_3")
				|| attrName.equals("contributor_name_4") || attrName
				.equals("contributor_name_5"));

		boolean contributorRoleAttr = false;
		contributorRoleAttr = (attrName.equals("contributor_role1")
				|| attrName.equals("contributor_role2")
				|| attrName.equals("contributor_role3")
				|| attrName.equals("contributor_role4")
				|| attrName.equals("contributor_role5")
				|| attrName.equals("contributor_role_1")
				|| attrName.equals("contributor_role_2")
				|| attrName.equals("contributor_role_3")
				|| attrName.equals("contributor_role_4") || attrName
				.equals("contributor_role_5"));

		if (contributorNameAttr) {
			return contributorNameAttr;
		}
		if (contributorRoleAttr) {
			return contributorRoleAttr;
		}
		return false;
	}

	/*
	 * private String formatErrorMessage(String name, String value, String
	 * typeName) { return "*** Error *** value length of Form attribute : '" +
	 * name +
	 * "' is bigger than the expected value length of Object attribute : '" +
	 * value + "' for object type : '" + typeName + "'"; }
	 */

}