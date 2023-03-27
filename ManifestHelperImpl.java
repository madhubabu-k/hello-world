package com.pearson.ps.ingest.helper;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Attr;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.documentum.fc.client.IDfFolder;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.DfTime;
import com.documentum.fc.common.IDfAttr;
import com.google.inject.Inject;
import com.pearson.ps.ingest.exception.IngestException;
import com.pearson.ps.ingest.main.IngestConstants;
import com.pearson.ps.ingest.provider.GenericPropertiesProvider;
import com.pearson.ps.ingest.provider.IngestLogProvider;
import com.pearson.ps.ingest.utils.DctmUtility;

/*****************************************************************************
 * 
 * File Name : ManifestHelper.java Purpose : Methods defined to handle
 * Manifest.xml Created On : 4th July,2011 Created By : HCL Technologies Ltd.
 * Modified On : Modified By : Changes Done: Remarks :
 * 
 *****************************************************************************/

public class ManifestHelperImpl implements ManifestHelper {

	private static final Logger LOGGER = LoggerFactory.getLogger(ManifestHelperImpl.class);

	private static final String ATTR_NAME = "name";
	private static final String ATTR_DATATYPE = "dataType";
	private static final String ATTR_PATH = "path";
	private static final String ATTR_VALUE = "value";

	private static final String TAG_FOLDER = "folder";
	private static final String TAG_FILE = "file";
	private static final String TAG_ATTRIBUTES = "attributes";
	private static final String TAG_ATTRIBUTE = "attribute";

	private static final String XPATH_PRODUCT_INFO = "//product_manifest/product_info";
	private static final String XPATH_METADATA = "//product_manifest/metadata";
	private static final String XPATH_PRODUCT_INFO_ATTRIBUTES = "//product_manifest/product_info/attributes";
	private static final String XPATH_PRODUCT_INFO_ATTRIBUTE = "//product_manifest/product_info/attributes/attribute";

	private IngestLogProvider ingestLogProvider;

	private GenericPropertiesProvider messageProps;
	
	/**
	 * Constructor in which the ingest log provider is injected by the framework
	 * 
	 * @param ingestLogProvider
	 */
	@Inject
	public ManifestHelperImpl(IngestLogProvider ingestLogProvider, GenericPropertiesProvider messagePropProvider) {
		this.ingestLogProvider = ingestLogProvider;
		this.messageProps = messagePropProvider;

	}

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#isManifestExist(java.lang
	 * .String)
	 */
	@Override
	public boolean isManifestExist(String manifestXMLFilePath) {

		boolean isManifestExist = false;
		File manifestXMLFile = new File(manifestXMLFilePath);
		isManifestExist = manifestXMLFile.exists();

		return isManifestExist;
	}

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#isProductInfoTagExist(org
	 * .w3c.dom.Document)
	 */
	@Override
	public boolean isProductInfoTagExist(Document manifestDocObject) throws IngestException {
		boolean isProductInfoTagExist = false;

		try {
			XPath xPathProdInfoTag = XPathFactory.newInstance().newXPath();
			XPathExpression xPathExprProdInfoTag = xPathProdInfoTag.compile(XPATH_PRODUCT_INFO);
			NodeList listProdInfoTag = (NodeList) xPathExprProdInfoTag.evaluate(manifestDocObject,
					XPathConstants.NODESET);

			if (listProdInfoTag.getLength() == 1) {
				isProductInfoTagExist = true;
			}
		} catch (Exception ex) {
			ingestLogProvider.appendFailedFilesMessage(messageProps.getProperty("product_info_missing"));
			throw new IngestException(messageProps.getProperty("product_info_missing"), ex);
		}
		return isProductInfoTagExist;
	}

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#isMetadataTagExist(org.w3c
	 * .dom.Document)
	 */
	@Override
	public boolean isMetadataTagExist(Document manifestDocObject) throws IngestException {
		boolean isProductInfoTagExist = false;

		try {
			XPath xPathMetadataTag = XPathFactory.newInstance().newXPath();
			XPathExpression xPathExprMetadataTag = xPathMetadataTag.compile(XPATH_METADATA);
			NodeList listMetadataTag = (NodeList) xPathExprMetadataTag.evaluate(manifestDocObject,
					XPathConstants.NODESET);

			if (listMetadataTag.getLength() == 1) {
				isProductInfoTagExist = true;
			}
		} catch (Exception ex) {
			ingestLogProvider.appendFailedFilesMessage(messageProps.getProperty("metadata_tag_missing"));
			throw new IngestException(messageProps.getProperty("metadata_tag_missing"), ex);
		}
		return isProductInfoTagExist;
	}

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#getManifestXMLDocObject(
	 * java.lang.String)
	 */
	@Override
	public Document getManifestXMLDocObject(String manifestXMLFilePath) throws IngestException {

		Document manifestDocObject = null;
		try {
			DocumentBuilderFactory objDomFactory = DocumentBuilderFactory.newInstance();
			objDomFactory.setNamespaceAware(true);
			DocumentBuilder objBuilder = objDomFactory.newDocumentBuilder();
			manifestDocObject = objBuilder.parse(manifestXMLFilePath);

		} catch (Exception ex) {
			LOGGER.error(ex.getMessage(), ex);
			throw new IngestException(ex.getMessage(), ex);
		}
		return manifestDocObject;
	}

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#setMetadataAttrsOnAssets
	 * (com.documentum.fc.client.IDfSysObject, org.w3c.dom.NodeList,
	 * java.lang.String, java.lang.String)
	 */
	@Override
	public void setMetadataAttrsOnAssets(IDfSysObject assetObj, NodeList listManifestAssests, String prodAssetName,
			String prodAssetPath) throws IngestException, DfException {

		boolean isAssetMappingFoundInManifest = false;

		Node nodeManifestAssest = null;

		for (int i = 0; i < listManifestAssests.getLength(); i++) {
			nodeManifestAssest = listManifestAssests.item(i);

			if (nodeManifestAssest.hasAttributes()) {
				NamedNodeMap manifestAssetAttrs = nodeManifestAssest.getAttributes();
				String manifestAssetName = "";
				String manifestAssetPath = "";
				if (manifestAssetAttrs.getNamedItem(ATTR_NAME) != null) {
					manifestAssetName = ((Attr) manifestAssetAttrs.getNamedItem(ATTR_NAME)).getValue();
				}
				if (manifestAssetAttrs.getNamedItem(ATTR_PATH) != null) {
					manifestAssetPath = ((Attr) manifestAssetAttrs.getNamedItem(ATTR_PATH)).getValue();
				}
				if (manifestAssetPath.equals(prodAssetPath)) {
					if (manifestAssetName.equals(prodAssetName)) {

						isAssetMappingFoundInManifest = true;
						break;
					}
				}
			}
		}

		if (isAssetMappingFoundInManifest) {
			if (nodeManifestAssest != null) {
				NodeList l_manifestAssetChildAttrs = nodeManifestAssest.getChildNodes();

				for (int i = 0; i < l_manifestAssetChildAttrs.getLength(); i++) {
					Node n_manifestAssetChildAttrs = l_manifestAssetChildAttrs.item(i);
					if (n_manifestAssetChildAttrs.getNodeName().equals(TAG_ATTRIBUTES)) {

						NodeList l_manifestAssetChildAttr = n_manifestAssetChildAttrs.getChildNodes();

						for (int j = 0; j < l_manifestAssetChildAttr.getLength(); j++) {
							Node n_manifestAssetChildAttr = l_manifestAssetChildAttr.item(j);
							if (n_manifestAssetChildAttr.getNodeName().equals(TAG_ATTRIBUTE)) {
								if (n_manifestAssetChildAttr.hasAttributes()) {
									NamedNodeMap m_manifestAssetChildAttrAttr = n_manifestAssetChildAttr
											.getAttributes();

									String[] manifestAssetAttrInfo = new String[3];
									// Pre-Initialze the manifestAssetAttrInfo
									// String Array with blank values so that
									// it can not be null.
									for (int item = 0; item < manifestAssetAttrInfo.length; item++) {
										manifestAssetAttrInfo[item] = "";
									}

									manifestAssetAttrInfo = getAssestAttrInfo(assetObj, manifestAssetAttrInfo,
											n_manifestAssetChildAttr, m_manifestAssetChildAttrAttr);
									setAssetAttrInfo(assetObj, manifestAssetAttrInfo);

								} 
							}
						}
						break;
					}
				}
			}
		} 
	}

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#setAssetAttrInfo(com.documentum
	 * .fc.client.IDfSysObject, java.lang.String[])
	 */
	@Override
	public void setAssetAttrInfo(IDfSysObject assetObj, String[] manifestAssetAttrInfo) throws IngestException, DfException {
		if (manifestAssetAttrInfo != null) {
			String manifestAssetAttrName = manifestAssetAttrInfo[0];
			String manifestAssetAttrDataType = manifestAssetAttrInfo[1];
			String manifestAssetAttrValue = manifestAssetAttrInfo[2];

			if (assetObj.hasAttr(manifestAssetAttrName)) {

				boolean isAttrRepeating = assetObj.isAttrRepeating(manifestAssetAttrName);

				int iDataType = assetObj.getAttrDataType(manifestAssetAttrName);
				String strAssetAttrDataType = DctmUtility.getDataTypeAsString(iDataType);
				int assetAttrLength = (assetObj.getAttr(assetObj.findAttrIndex(manifestAssetAttrName))).getLength();

				if (manifestAssetAttrDataType.equalsIgnoreCase(strAssetAttrDataType)) {

					setAttrValueBasedOnDataType(assetObj, manifestAssetAttrName, iDataType, manifestAssetAttrValue,
							assetAttrLength, isAttrRepeating);

				} 

			} 
		}
	}

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#getAssestAttrInfo(com.documentum
	 * .fc.client.IDfSysObject, java.lang.String[], org.w3c.dom.Node,
	 * org.w3c.dom.NamedNodeMap)
	 */
	@Override
	public String[] getAssestAttrInfo(IDfSysObject assetObj, String[] manifestAssetAttrInfo,
			Node n_manifestAssetChildAttr, NamedNodeMap m_manifestAssetChildAttrAttr) throws IngestException, DOMException, DfException {

		if (m_manifestAssetChildAttrAttr.getNamedItem(ATTR_NAME) != null) {
			manifestAssetAttrInfo[0] = ((Attr) m_manifestAssetChildAttrAttr.getNamedItem(ATTR_NAME)).getValue();

			if (assetObj.hasAttr(manifestAssetAttrInfo[0])) {

				if (m_manifestAssetChildAttrAttr.getNamedItem(ATTR_DATATYPE) != null) {
					manifestAssetAttrInfo[1] = ((Attr) m_manifestAssetChildAttrAttr.getNamedItem(ATTR_DATATYPE))
							.getValue();
				} 

				NodeList l_manifestAssetChildAttrChildValue = n_manifestAssetChildAttr.getChildNodes();
				for (int k = 0; k < l_manifestAssetChildAttrChildValue.getLength(); k++) {
					Node n_manifestAssetChildAttrChildValue = l_manifestAssetChildAttrChildValue.item(k);
					if (n_manifestAssetChildAttrChildValue.getNodeName().equals(ATTR_VALUE)) {
						manifestAssetAttrInfo[2] += n_manifestAssetChildAttrChildValue.getTextContent() + ",";
					}
				}

			}
		} 

		return manifestAssetAttrInfo;
	}

	/*
	 * 
	 * @see com.pearson.ps.ingest.helper.IManifesthelper#
	 * checkAllMandatoryAttrInfoForProductInManifest
	 * (com.documentum.fc.client.IDfFolder, org.w3c.dom.NodeList,
	 * java.util.List)
	 */
	@Override
	public boolean checkAllMandatoryAttrInfoForProductInManifest(IDfFolder prodFolderObj, NodeList listProductInfoAttr,
			List<String> mandatoryProdInfoAttrNamesList) throws IngestException, DfException {

		boolean isAllMandatoryAttrsFoundInManifest = false;

		List<String[]> productInfoAllAttrsInfo = new ArrayList<String[]>();
		String[] productInfoAllAttrNames = new String[listProductInfoAttr.getLength()];
		String[] productInfoAllAttrDataTypes = new String[listProductInfoAttr.getLength()];
		String[] productInfoAllAttrValues = new String[listProductInfoAttr.getLength()];

		// Pre-initailize the String Arrays with blank values so that they can
		// not be null.
		for (int i = 0; i < listProductInfoAttr.getLength(); i++) {

			productInfoAllAttrNames[i] = "";
			productInfoAllAttrDataTypes[i] = "";
			productInfoAllAttrValues[i] = "";
		}

		productInfoAllAttrsInfo = getProductInfoAllAttrsInfo(prodFolderObj, listProductInfoAttr,
				productInfoAllAttrsInfo, productInfoAllAttrNames, productInfoAllAttrDataTypes, productInfoAllAttrValues);

		if (null != productInfoAllAttrsInfo) {

			productInfoAllAttrNames = productInfoAllAttrsInfo.get(0);
			productInfoAllAttrDataTypes = productInfoAllAttrsInfo.get(1);
			productInfoAllAttrValues = productInfoAllAttrsInfo.get(2);

			boolean isMandatoryAttrInfoCorrect = false;

			for (int m_attrCount = 0; m_attrCount < mandatoryProdInfoAttrNamesList.size(); m_attrCount++) {
				String mandatoryAttrName = (String) mandatoryProdInfoAttrNamesList.get(m_attrCount);
				String mandatoryAttrDataType = "";
				int mandatoryAttrLength = 0;

				if (prodFolderObj.hasAttr(mandatoryAttrName)) {
					int iAttrDatatype = prodFolderObj.getAttrDataType(mandatoryAttrName);
					mandatoryAttrDataType = DctmUtility.getDataTypeAsString(iAttrDatatype);
					mandatoryAttrLength = (prodFolderObj.getAttr(prodFolderObj.findAttrIndex(mandatoryAttrName)))
							.getLength();

				} else {
					ingestLogProvider.appendFailedFilesMessage(String.format(
							messageProps.getProperty("mand_attr_missing"), mandatoryAttrName));
					throw new IngestException(String.format(messageProps.getProperty("mand_attr_missing"),
							mandatoryAttrName));
				}

				int attrCount = 0;
				boolean isMandatoryAttrNameFound = false;
				for (attrCount = 0; attrCount < productInfoAllAttrNames.length; attrCount++) {

					String manifestAttrName = productInfoAllAttrNames[attrCount];
					if (manifestAttrName != null && !manifestAttrName.equals("")
							&& mandatoryAttrName.equalsIgnoreCase(manifestAttrName)) {

						isMandatoryAttrNameFound = true;
						break;
					}
				}
				if (isMandatoryAttrNameFound) {
					String manifestAttrDataType = productInfoAllAttrDataTypes[attrCount];

					if (manifestAttrDataType != null && mandatoryAttrDataType.equalsIgnoreCase(manifestAttrDataType)) {

						boolean isMandatoryAttrRepeating = prodFolderObj.isAttrRepeating(mandatoryAttrName);

						String manifestAttrValue = productInfoAllAttrValues[attrCount];

						if (isMandatoryAttrRepeating) {

							String[] manifestAttrSingleValue = manifestAttrValue.split(",");


							for (int k = 0; k < manifestAttrSingleValue.length; k++) {
								if (manifestAttrSingleValue != null && !manifestAttrSingleValue[k].equalsIgnoreCase("")) {

									if (manifestAttrSingleValue.length <= mandatoryAttrLength) {
										isMandatoryAttrInfoCorrect = true;
									} else {
										String errorMessage = String.format(
												messageProps.getProperty("mand_attr_length"), mandatoryAttrName);
										ingestLogProvider.appendFailedFilesMessage(errorMessage);
										throw new IngestException(errorMessage);
									}
								} else {
									String errorMessage = String.format(
											messageProps.getProperty("mand_attr_null"), mandatoryAttrName);
									ingestLogProvider.appendFailedFilesMessage(errorMessage);
									throw new IngestException(errorMessage);
								}
							}
						} else {
							manifestAttrValue = manifestAttrValue.replaceFirst(",", "");
							int commaIndex = manifestAttrValue.indexOf(',');

							if (manifestAttrValue != null && !manifestAttrValue.equalsIgnoreCase("")
									&& commaIndex == -1) {

								if (manifestAttrValue.length() <= mandatoryAttrLength) {
									isMandatoryAttrInfoCorrect = true;
								
								} else {
									String errorMessage = String.format(
											messageProps.getProperty("mand_attr_length"), mandatoryAttrName);
									ingestLogProvider.appendFailedFilesMessage(errorMessage);
									throw new IngestException(errorMessage);
								}
							} else {
								ingestLogProvider.appendFailedFilesMessage(String.format(
										messageProps.getProperty("mand_attr_not_repeat"), mandatoryAttrName));
								throw new IngestException(String.format(
										messageProps.getProperty("mand_attr_not_repeat"), mandatoryAttrName));
							}
						}
					} else {
						ingestLogProvider.appendFailedFilesMessage(String.format(
								messageProps.getProperty("mand_attr_type_mismatch"), mandatoryAttrName));
						throw new IngestException(String.format(messageProps.getProperty("mand_attr_type_mismatch"),
								mandatoryAttrName));
					}
				} else {
					ingestLogProvider.appendFailedFilesMessage(String.format(
							messageProps.getProperty("mand_attr_not_found"), mandatoryAttrName));
					throw new IngestException(String.format(messageProps.getProperty("mand_attr_not_found"),
							mandatoryAttrName));
				}
			}
			if (isMandatoryAttrInfoCorrect) {
				isAllMandatoryAttrsFoundInManifest = true;
			}
		} else {
			ingestLogProvider.appendFailedFilesMessage(messageProps.getProperty("prod_info_null"));
			throw new IngestException(messageProps.getProperty("prod_info_null"));
		}

		return isAllMandatoryAttrsFoundInManifest;
	}

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#getProductInfoAllAttrsInfo
	 * (com.documentum.fc.client.IDfFolder, org.w3c.dom.NodeList,
	 * java.util.ArrayList, java.lang.String[], java.lang.String[],
	 * java.lang.String[])
	 */
	@Override
	public List<String[]> getProductInfoAllAttrsInfo(IDfFolder prodFolderObj, NodeList listProductInfoAttr,
			List<String[]> prodocutInfoAllAttrsInfo, String[] productInfoAllAttrNames,
			String[] productInfoAllAttrDataTypes, String[] productInfoAllAttrValues) throws IngestException {

		try {
			int attrInfoCount = 0;

			for (int i = 0; i < listProductInfoAttr.getLength(); i++) {
				Node nodeProductInfoAttr = listProductInfoAttr.item(i);

				if (nodeProductInfoAttr.hasAttributes()) {
					NamedNodeMap prodInfoAttrMap = nodeProductInfoAttr.getAttributes();

					if (prodInfoAttrMap.getNamedItem(ATTR_NAME) != null) {
						productInfoAllAttrNames[attrInfoCount] = prodInfoAttrMap.getNamedItem(ATTR_NAME).getNodeValue();

						if (prodFolderObj.hasAttr(productInfoAllAttrNames[attrInfoCount])) {
							boolean isAttrRepeating = prodFolderObj
									.isAttrRepeating(productInfoAllAttrNames[attrInfoCount]);

							if (prodInfoAttrMap.getNamedItem(ATTR_DATATYPE) != null) {
								productInfoAllAttrDataTypes[attrInfoCount] = prodInfoAttrMap
										.getNamedItem(ATTR_DATATYPE).getNodeValue();
							} else {
								ingestLogProvider.appendFooterMessage(messageProps.getProperty("prod_info_datatype"));
							}

							boolean isValueTagFound = false;
							int valueTagCount = 0;
							NodeList listProdInfoAttrValue = nodeProductInfoAttr.getChildNodes();

							for (int j = 0; j < listProdInfoAttrValue.getLength(); j++) {
								Node nodeProdInfoAttrValue = listProdInfoAttrValue.item(j);
								if (nodeProdInfoAttrValue.getNodeName().equals(ATTR_VALUE)) {
									isValueTagFound = true;
									valueTagCount++;
									productInfoAllAttrValues[attrInfoCount] += nodeProdInfoAttrValue.getTextContent()
											+ ",";
								}
							}
							if (!isValueTagFound) {
								ingestLogProvider.appendFooterMessage(messageProps.getProperty("value_tag_missing"));
							} else {
								if (!isAttrRepeating && valueTagCount != 1) {
									ingestLogProvider.appendFooterMessage(messageProps.getProperty("tag_mult_values"));
								}
							}
						}
					} else {
						ingestLogProvider.appendFooterMessage(messageProps.getProperty("name_tag_missing"));
					}
					attrInfoCount++;
				} else {
					ingestLogProvider.appendFooterMessage(messageProps.getProperty("tag_missing_attr"));
				}
			}
			prodocutInfoAllAttrsInfo.add(productInfoAllAttrNames);
			prodocutInfoAllAttrsInfo.add(productInfoAllAttrDataTypes);
			prodocutInfoAllAttrsInfo.add(productInfoAllAttrValues);

		} catch (Exception ex) {
			throw new IngestException(ex.getMessage(), ex);
		}
		return prodocutInfoAllAttrsInfo;
	}

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#setProductInfoAttrsOnProduct
	 * (com.documentum.fc.client.IDfFolder, org.w3c.dom.NodeList)
	 */
	@Override
	public void setProductInfoAttrsOnProduct(IDfFolder prodFolderObj, NodeList listProductInfoAttr) throws IngestException, DfException {

		List<String[]> prodocutInfoAllAttrsInfo = new ArrayList<String[]>();
		String[] productInfoAllAttrNames = new String[listProductInfoAttr.getLength()];
		String[] productInfoAllAttrDataTypes = new String[listProductInfoAttr.getLength()];
		String[] productInfoAllAttrValues = new String[listProductInfoAttr.getLength()];

		// Pre-initailize the String Arrays with blank values so that they can
		// not be null.
		for (int i = 0; i < listProductInfoAttr.getLength(); i++) {

			productInfoAllAttrNames[i] = "";
			productInfoAllAttrDataTypes[i] = "";
			productInfoAllAttrValues[i] = "";
		}

		prodocutInfoAllAttrsInfo = getProductInfoAllAttrsInfo(prodFolderObj, listProductInfoAttr,
				prodocutInfoAllAttrsInfo, productInfoAllAttrNames, productInfoAllAttrDataTypes,
				productInfoAllAttrValues);

		if (null != prodocutInfoAllAttrsInfo) {

			productInfoAllAttrNames = prodocutInfoAllAttrsInfo.get(0);
			productInfoAllAttrDataTypes = prodocutInfoAllAttrsInfo.get(1);
			productInfoAllAttrValues = prodocutInfoAllAttrsInfo.get(2);

			for (int attrCount = 0; attrCount < productInfoAllAttrNames.length; attrCount++) {
				String manifestAttrName = productInfoAllAttrNames[attrCount];
				//Skip profit_center attribute as it is a unique case
				if (manifestAttrName.equalsIgnoreCase(IngestConstants.ATTR_BU_NAME))
					continue;
				String manifestAttrDataType = productInfoAllAttrDataTypes[attrCount];
				String manifestAttrValue = productInfoAllAttrValues[attrCount];

				if (manifestAttrName != null && !manifestAttrName.equals("") && prodFolderObj.hasAttr(manifestAttrName)) {

					boolean isAttrRepeating = prodFolderObj.isAttrRepeating(manifestAttrName);

					int iDataType = prodFolderObj.getAttrDataType(manifestAttrName);
					String strFolderAttrDataType = DctmUtility.getDataTypeAsString(iDataType);
					int folderAttrLength = (prodFolderObj.getAttr(prodFolderObj.findAttrIndex(manifestAttrName)))
							.getLength();

					if (manifestAttrDataType.equalsIgnoreCase(strFolderAttrDataType)) {

						setAttrValueBasedOnDataType(prodFolderObj, manifestAttrName, iDataType, manifestAttrValue,
								folderAttrLength, isAttrRepeating);

					} else {
						ingestLogProvider.appendFooterMessage(String.format(
								messageProps.getProperty("folder_attr_mismatch"), manifestAttrDataType));
					}
				} else {
					ingestLogProvider.appendFooterMessage(String.format(messageProps.getProperty("attr_not_valid"),
							manifestAttrName, prodFolderObj.getObjectName(), prodFolderObj.getTypeName()));
				}
			}
		}
	}

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#setAttrValueBasedOnDataType
	 * (com.documentum.fc.client.IDfSysObject, java.lang.String, int,
	 * java.lang.String, int, boolean)
	 */
	@Override
	public void setAttrValueBasedOnDataType(IDfSysObject sysObj, String attrName, int iAttrDataType, String attrValue,
			int sysObjAttrLength, boolean isAttrRepeating) throws DfException, IngestException {
		try {
			if (isAttrRepeating) {

				String[] repeatAttrValues = attrValue.split(",");

				for (int counter = 0; counter < repeatAttrValues.length; counter++) {
					String loAttrValue = repeatAttrValues[counter];

					switch (iAttrDataType) {

					case IDfAttr.DM_STRING:

						if (loAttrValue.length() <= sysObjAttrLength) {
							if (DctmUtility.isObjectValueValid(sysObj, attrName, loAttrValue)) {
								sysObj.setRepeatingString(attrName, counter, loAttrValue);
							} 
						} 
						break;
					case IDfAttr.DM_TIME:
						sysObj.setRepeatingTime(attrName, counter, new DfTime(loAttrValue,
								IngestConstants.DEFAULT_TIME_FORMAT));
						break;
					case IDfAttr.DM_BOOLEAN:
						sysObj.setRepeatingBoolean(attrName, counter, Boolean.parseBoolean(loAttrValue));
						break;
					case IDfAttr.DM_INTEGER:
						sysObj.setRepeatingInt(attrName, counter, Integer.parseInt(loAttrValue));
						break;
					case IDfAttr.DM_DOUBLE:
						sysObj.setRepeatingDouble(attrName, counter, Double.parseDouble(loAttrValue));
						break;
					case IDfAttr.DM_ID:
						sysObj.setRepeatingId(attrName, counter, new DfId(loAttrValue));
						break;
					default:
						break;
					}
				}
			} else {
				String loAttrValue = attrValue.replaceFirst(",", "");
				int commaIndex = loAttrValue.indexOf(',');

				if (commaIndex == -1) {

					switch (iAttrDataType) {

					case IDfAttr.DM_STRING:

						if (loAttrValue.length() <= sysObjAttrLength) {

							if (DctmUtility.isObjectValueValid(sysObj, attrName, loAttrValue)) {
								sysObj.setString(attrName, loAttrValue);
							} 
						} 
						break;

					case IDfAttr.DM_TIME:
						sysObj.setTime(attrName, new DfTime(loAttrValue, IngestConstants.DEFAULT_TIME_FORMAT));
						break;
					case IDfAttr.DM_BOOLEAN:
						sysObj.setBoolean(attrName, Boolean.parseBoolean(loAttrValue));
						break;
					case IDfAttr.DM_INTEGER:
						sysObj.setInt(attrName, Integer.parseInt(loAttrValue));
						break;
					case IDfAttr.DM_DOUBLE:
						sysObj.setDouble(attrName, Double.parseDouble(loAttrValue));
						break;
					case IDfAttr.DM_ID:
						sysObj.setId(attrName, new DfId(loAttrValue));
						break;
					default:
						break;
					}
				} 
			}
		} catch (DfException dfe) {
			throw new IngestException(dfe.getMessage(), dfe);
		}
	}

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#getProductInfoAttrNodeList
	 * (org.w3c.dom.Document)
	 */
	@Override
	public NodeList getProductInfoAttrNodeList(Document manifestDocObject) throws IngestException {
		NodeList listProdInfoAttr = null;

		try {
			XPath xPathProdInfoAttrs = XPathFactory.newInstance().newXPath();
			XPathExpression xPathExprProdInfoAttrs = xPathProdInfoAttrs.compile(XPATH_PRODUCT_INFO_ATTRIBUTES);
			NodeList listProdInfoAttrs = (NodeList) xPathExprProdInfoAttrs.evaluate(manifestDocObject,
					XPathConstants.NODESET);

			if (listProdInfoAttrs.getLength() != 1) {
				ingestLogProvider.appendFailedFilesMessage(messageProps.getProperty("tag_not_single"));
				throw new IngestException(messageProps.getProperty("tag_not_single"));

			} else {
				XPath xPathProdInfoAttr = XPathFactory.newInstance().newXPath();
				XPathExpression xPathExprProdInfoAttr = xPathProdInfoAttr.compile(XPATH_PRODUCT_INFO_ATTRIBUTE);
				listProdInfoAttr = (NodeList) xPathExprProdInfoAttr.evaluate(manifestDocObject, XPathConstants.NODESET);

				if (!(listProdInfoAttr.getLength() >= 1)) {
					ingestLogProvider.appendFailedFilesMessage(messageProps.getProperty("tag_not_present"));
					throw new IngestException(messageProps.getProperty("tag_not_present"));

				}
			}
		} catch (Exception ex) {
			throw new IngestException(ex.getMessage(), ex);
		}
		return listProdInfoAttr;
	}

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#getMetadataFolderNodeList
	 * (org.w3c.dom.Document)
	 */
	@Override
	public NodeList getMetadataFolderNodeList(Document manifestDocObject) throws IngestException {
		NodeList listMetaDataFolders = null;
		try {
			listMetaDataFolders = manifestDocObject.getElementsByTagName(TAG_FOLDER);
		} catch (Exception ex) {
			throw new IngestException(ex.getMessage(), ex);
		}
		return listMetaDataFolders;
	}

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#getMetaDataFileNodeList(
	 * org.w3c.dom.Document)
	 */
	@Override
	public NodeList getMetaDataFileNodeList(Document manifestDocObject) throws IngestException {
		NodeList listMetaDataFiles = null;
		try {
			listMetaDataFiles = manifestDocObject.getElementsByTagName(TAG_FILE);
		} catch (Exception ex) {
			throw new IngestException(ex.getMessage(), ex);
		}
		return listMetaDataFiles;
	}

	/**
	 * Method to fetch the value of the profit center from the manifest.xml 
	 * 
	 */
	public String getProfitCenterFromManifest(String manifestFilePath) {
		String buName = "";
		Document manifestDocObject = null;
		try {
			DocumentBuilderFactory objDomFactory = DocumentBuilderFactory.newInstance();
			objDomFactory.setNamespaceAware(true);
			DocumentBuilder objBuilder = objDomFactory.newDocumentBuilder();
			manifestDocObject = objBuilder.parse(manifestFilePath);		

			XPath attributeTag = XPathFactory.newInstance().newXPath();
			XPathExpression attrTagExpr = attributeTag.compile(XPATH_PRODUCT_INFO_ATTRIBUTE);
			NodeList attrTagNodeList = (NodeList) attrTagExpr.evaluate(manifestDocObject,
					XPathConstants.NODESET);

			for (int attrCount =0; attrCount < attrTagNodeList.getLength(); attrCount++ ) {
				Node node = attrTagNodeList.item(attrCount);
				NamedNodeMap manifestAssetAttrs = node.getAttributes();
				if (manifestAssetAttrs.getNamedItem(ATTR_NAME)!= null && 
						manifestAssetAttrs.getNamedItem(ATTR_NAME).getNodeValue().equals(IngestConstants.PRSN_PS_FOLDER_ATTR_PRSN_BUSINESS)) {
					NodeList nodes = node.getChildNodes();
					for (int nodeCount = 0; nodeCount < nodes.getLength(); nodeCount++) {
						if (nodes.item(nodeCount).getNodeName().equals(ATTR_VALUE)) {
							buName = nodes.item(nodeCount).getTextContent();
						}
					}
				}
				
			}
		} catch (Exception ex) {
			LOGGER.error("Error fetching BU from Manifest.XML ",ex);
		}		
		return buName;
	}	
	
}
