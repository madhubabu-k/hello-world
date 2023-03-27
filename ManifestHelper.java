package com.pearson.ps.ingest.helper;

import java.util.List;

import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.documentum.fc.client.IDfFolder;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.common.DfException;
import com.pearson.ps.ingest.exception.IngestException;

public interface ManifestHelper {

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#isManifestExist(java.lang
	 * .String)
	 */
	boolean isManifestExist(String manifestXMLFilePath);

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#isProductInfoTagExist(org
	 * .w3c.dom.Document)
	 */
	boolean isProductInfoTagExist(Document manifestDocObject) throws IngestException;

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#isMetadataTagExist(org.w3c
	 * .dom.Document)
	 */
	boolean isMetadataTagExist(Document manifestDocObject) throws IngestException;

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#getManifestXMLDocObject(
	 * java.lang.String)
	 */
	Document getManifestXMLDocObject(String manifestXMLFilePath) throws IngestException;

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#setMetadataAttrsOnAssets
	 * (com.documentum.fc.client.IDfSysObject, org.w3c.dom.NodeList,
	 * java.lang.String, java.lang.String)
	 */
	void setMetadataAttrsOnAssets(IDfSysObject assetObj, NodeList listManifestAssests,
			String prodAssetName, String prodAssetPath) throws IngestException, DfException;

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#setAssetAttrInfo(com.documentum
	 * .fc.client.IDfSysObject, java.lang.String[])
	 */
	void setAssetAttrInfo(IDfSysObject assetObj, String[] manifestAssetAttrInfo)
			throws IngestException, DfException;

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#getAssestAttrInfo(com.documentum
	 * .fc.client.IDfSysObject, java.lang.String[], org.w3c.dom.Node,
	 * org.w3c.dom.NamedNodeMap)
	 */
	String[] getAssestAttrInfo(IDfSysObject assetObj, String[] manifestAssetAttrInfo,
			Node n_manifestAssetChildAttr, NamedNodeMap m_manifestAssetChildAttrAttr) throws IngestException,
			DOMException, DfException;

	/*
	 * 
	 * @see com.pearson.ps.ingest.helper.IManifesthelper#
	 * checkAllMandatoryAttrInfoForProductInManifest
	 * (com.documentum.fc.client.IDfFolder, org.w3c.dom.NodeList,
	 * java.util.List)
	 */
	boolean checkAllMandatoryAttrInfoForProductInManifest(IDfFolder prodFolderObj,
			NodeList listProductInfoAttr, List<String> mandatoryProdInfoAttrNamesList) throws IngestException, DfException;

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#getProductInfoAllAttrsInfo
	 * (com.documentum.fc.client.IDfFolder, org.w3c.dom.NodeList,
	 * java.util.ArrayList, java.lang.String[], java.lang.String[],
	 * java.lang.String[])
	 */
	List<String[]> getProductInfoAllAttrsInfo(IDfFolder prodFolderObj,
			NodeList listProductInfoAttr, List<String[]> prodocutInfoAllAttrsInfo,
			String[] productInfoAllAttrNames, String[] productInfoAllAttrDataTypes, String[] productInfoAllAttrValues)
			throws IngestException;

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#setProductInfoAttrsOnProduct
	 * (com.documentum.fc.client.IDfFolder, org.w3c.dom.NodeList)
	 */
	void setProductInfoAttrsOnProduct(IDfFolder prodFolderObj, NodeList listProductInfoAttr)
			throws IngestException, DfException;

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#setAttrValueBasedOnDataType
	 * (com.documentum.fc.client.IDfSysObject, java.lang.String, int,
	 * java.lang.String, int, boolean)
	 */
	void setAttrValueBasedOnDataType(IDfSysObject sysObj, String attrName, int iAttrDataType,
			String attrValue, int sysObjAttrLength, boolean isAttrRepeating) throws DfException, IngestException;

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#getProductInfoAttrNodeList
	 * (org.w3c.dom.Document)
	 */
	NodeList getProductInfoAttrNodeList(Document manifestDocObject) throws IngestException;

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#getMetadataFolderNodeList
	 * (org.w3c.dom.Document)
	 */
	NodeList getMetadataFolderNodeList(Document manifestDocObject) throws IngestException;

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.helper.IManifesthelper#getMetaDataFileNodeList(
	 * org.w3c.dom.Document)
	 */
	NodeList getMetaDataFileNodeList(Document manifestDocObject) throws IngestException;
	
	/*
	 * Fetches the value of business unit from the manifest file 
	 */
	public String getProfitCenterFromManifest(String filePath);
}