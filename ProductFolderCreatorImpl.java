/**
 * 
 */
package com.pearson.ps.ingest.creator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import com.documentum.com.DfClientX;
import com.documentum.fc.client.IDfACL;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfFolder;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.client.IDfTypedObject;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.IDfId;
import com.google.inject.Inject;
import com.pearson.ps.ingest.exception.IngestException;
import com.pearson.ps.ingest.helper.ManifestHelper;
import com.pearson.ps.ingest.main.IngestConstants;
import com.pearson.ps.ingest.provider.CachedDataProvider;
import com.pearson.ps.ingest.provider.GenericPropertiesProvider;
import com.pearson.ps.ingest.provider.GenericPropertiesProviderImpl;
import com.pearson.ps.ingest.provider.IngestJobConfigProvider;
import com.pearson.ps.ingest.provider.IngestLogProvider;
import com.pearson.ps.ingest.utils.DctmACLUtil;
import com.pearson.ps.ingest.utils.DctmUtility;
import com.pearson.ps.ingest.utils.IngestUtility;

/**
 * Class to create the product folder
 * 
 * @author Manav Leslie
 * 
 */
public class ProductFolderCreatorImpl extends AbstractIngestCreator implements
		ProductFolderCreator {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(ProductFolderCreatorImpl.class);

	private CachedDataProvider cachedDataProvider;
	private IngestLogProvider ingestLogProvider;
	private ManifestHelper manifestHelper;
	private IngestJobConfigProvider ingestJobConfigProvider;
	private GenericPropertiesProvider messagesPropProvider;

	/**
	 * Constructor for the class. Guice framework will set these attributes when
	 * the object is initiated using the framework
	 * 
	 * @param cachedDataProvider
	 *            Cached data provider class
	 * @param manifesthelper
	 *            Helper class to read the manifest files
	 * @param ingestLogProvider
	 *            Ingest log provider class
	 * @param ingestJobConfigProvider
	 *            Class to provide session details
	 */
	@Inject
	public ProductFolderCreatorImpl(CachedDataProvider cachedDataProvider,
			ManifestHelper manifesthelper, IngestLogProvider ingestLogProvider,
			IngestJobConfigProvider ingestJobConfigProvider,
			GenericPropertiesProvider messagesPropProvider) {
		this.cachedDataProvider = cachedDataProvider;
		this.manifestHelper = manifesthelper;
		this.ingestLogProvider = ingestLogProvider;
		this.ingestJobConfigProvider = ingestJobConfigProvider;
		this.messagesPropProvider = messagesPropProvider;
	}

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.creator.IProductFolderCreator#createProductFolder
	 * (com.documentum.fc.client.IDfSysObject,
	 * com.documentum.fc.client.IDfSession, com.documentum.fc.common.IDfId)
	 */
	@Override
	public IDfId createProductFolder(IDfSysObject loFormReqNoObj,
			IDfSession ingestSession, IDfId lifecycleId) throws DfException,
			IngestException, IOException {

		String productNameWithPath = loFormReqNoObj.getString("product_name");

		productNameWithPath = productNameWithPath.replaceAll("\\\\", "/");

		String srcFolderPath = loFormReqNoObj.getString("source_folder") + "/"
				+ productNameWithPath;

		boolean isManifestRequired = false;

		if (loFormReqNoObj.getString(IngestConstants.ATTR_MANIFEST_REQUIRED)
				.equalsIgnoreCase("T")) {
			isManifestRequired = true;
		}

		IDfFolder loFoldrObj = (IDfFolder) ingestSession
				.getObject(loFormReqNoObj.getId("prod_dest"));
		String strlsProdDestFldrPath = loFoldrObj.getRepeatingString(
				"r_folder_path", 0);
		if (!DctmUtility.isDctmObjectPresent(ingestSession,
				strlsProdDestFldrPath)) {
			LOGGER.error("Destination folder doesnt exist {}",
					strlsProdDestFldrPath);
			ingestLogProvider.appendFailedFilesMessage(String.format(
					messagesPropProvider.getProperty("prod_folder_missing"),
					strlsProdDestFldrPath));
			throw new IngestException(String.format(messagesPropProvider
					.getProperty("prod_folder_missing"), strlsProdDestFldrPath));
		}

		String productName = productNameWithPath.substring(productNameWithPath
				.lastIndexOf("/") + 1);

		String fullProductFolderPath = strlsProdDestFldrPath + "/"
				+ productName;
		LOGGER.info("fullProductFolderPath  {}", fullProductFolderPath);
		if (DctmUtility.isDctmObjectPresent(ingestSession,
				fullProductFolderPath)) {
			LOGGER.error("Product folder already exists {}",
					fullProductFolderPath);
			ingestLogProvider.appendFailedFilesMessage(String.format(
					messagesPropProvider.getProperty("prod_already_exists"),
					fullProductFolderPath));
			throw new IngestException(String.format(messagesPropProvider
					.getProperty("prod_already_exists"), fullProductFolderPath));
		}

		String buName = "";

		// In case of manifest.XML, profit center must be defined in that.
		if (isManifestRequired) {
			String manifestXMLFilePath = srcFolderPath + "/"
					+ IngestConstants.PRODUCT_MANIFEST_FILENAME;
			boolean isManifestPresent = manifestHelper
					.isManifestExist(manifestXMLFilePath);

			if (isManifestPresent) {
				buName = manifestHelper
						.getProfitCenterFromManifest(manifestXMLFilePath);
			} else {
				logExceptionAndThrow(loFormReqNoObj, String.format(
						messagesPropProvider.getProperty("meta_inf_missing"),
						manifestXMLFilePath));
			}

		} else {
			buName = loFormReqNoObj.getString(IngestConstants.ATTR_BU_NAME);
		}

		if (StringUtils.isEmpty(buName)) {
			LOGGER.error("Unable to resolve the Business Unit.");
			throw new IngestException(messagesPropProvider
					.getProperty("business_unit_empty"));
		}
		cachedDataProvider.populateExtensionMappingMap(buName, ingestSession);

		// Map<String, String> prop =
		// cachedDataProvider.loadMetaFromProps(IngestUtility.getBUKey(buName));

		GenericPropertiesProvider prodTypeProp = new GenericPropertiesProviderImpl(
				IngestConstants.PRSN_PROD_TYPE_PROP_FILE);
		String productTypeFolder = prodTypeProp.getProperty(IngestUtility
				.getBUKey(buName).toUpperCase());
		IDfFolder prodFolderObj = (IDfFolder) ingestSession
				.newObject(productTypeFolder);
		prodFolderObj.setObjectName(productName);
		prodFolderObj.link(strlsProdDestFldrPath);

		if (isManifestRequired) {

			String manifestXMLFilePath = srcFolderPath + "/"
					+ IngestConstants.PRODUCT_MANIFEST_FILENAME;

			Document manifestDocObject = manifestHelper
					.getManifestXMLDocObject(manifestXMLFilePath);
			boolean isProductInfoTagExist = manifestHelper
					.isProductInfoTagExist(manifestDocObject);

			if (isProductInfoTagExist) {

				NodeList listProductInfoAttr = manifestHelper
						.getProductInfoAttrNodeList(manifestDocObject);

				if (listProductInfoAttr != null) {
					List<String> mandatoryProdInfoAttrNamesList = new ArrayList<String>();
					mandatoryProdInfoAttrNamesList = new GenericPropertiesProviderImpl(
							IngestConstants.MANDATORY_ATTRIBUTES_FILE)
							.getAllKeysAsArray();

					boolean isAllMandatoryAttrsFoundInManifest = manifestHelper
							.checkAllMandatoryAttrInfoForProductInManifest(
									prodFolderObj, listProductInfoAttr,
									mandatoryProdInfoAttrNamesList);

					if (isAllMandatoryAttrsFoundInManifest) {
						manifestHelper.setProductInfoAttrsOnProduct(
								prodFolderObj, listProductInfoAttr);

					} else {
						logExceptionAndThrow(loFormReqNoObj,
								messagesPropProvider
										.getProperty("prod_info_attr_missing"));
					}
				} else {
					logExceptionAndThrow(loFormReqNoObj, messagesPropProvider
							.getProperty("prod_attr_null"));
				}
			} else {
				logExceptionAndThrow(loFormReqNoObj, messagesPropProvider
						.getProperty("xml_prod_error"));
			}

		} else {
			String strPropertyFileName = cachedDataProvider
					.getPropertyFileName(IngestUtility.getBUKey(buName)
							.toUpperCase(), false);

			if (strPropertyFileName.equalsIgnoreCase("NO MATCH")) {
				throw new IngestException(messagesPropProvider
						.getProperty("metadata_properties_file_missing"));
			} else {
				GenericPropertiesProvider prodMetaDataPropProvider = new GenericPropertiesProviderImpl(
						strPropertyFileName);
				Map<String, String> prodMetaDataProp = prodMetaDataPropProvider
						.getAllEntrySetAsArray();
				List<String> orderedKeysList = prodMetaDataPropProvider
						.getAllKeysAsOrderedList();

				addSysObjectAttributes(ingestSession, prodFolderObj,
						prodMetaDataProp, orderedKeysList, loFormReqNoObj);
			}
		}

		prodFolderObj.save();
		String folderId = prodFolderObj.getObjectId().getId();

		// Fetching the Product Folder just after Save() to do further
		// operations on Product.
		prodFolderObj = (IDfFolder) ingestSession.getObject(new DfId(folderId));

		prodFolderObj.attachPolicy(lifecycleId,
				IngestConstants.IN_PROGRESS_STATE, "");

		String mappedFolderAttrName = getMappedAttrForProdRef(ingestSession,
				loFormReqNoObj.getString("prod_ref"));

		createDMRelation(folderId, mappedFolderAttrName, loFormReqNoObj
				.getString("related_prod"), buName, ingestSession);

		IDfACL ingestJobACLObj = DctmACLUtil.getProductFolderAcl(folderId,
				ingestJobConfigProvider.getAdminSession(),
				buName.toUpperCase(), loFormReqNoObj
						.getString("security_profile"));

		if (null != ingestJobACLObj) {
			prodFolderObj.setACL(ingestJobACLObj);
			prodFolderObj.setString("prsn_security_profile", loFormReqNoObj
					.getString("security_profile"));
		} else {
			ingestLogProvider.appendFailedFilesMessage(messagesPropProvider
					.getProperty("acl_not_created"));
			throw new IngestException(messagesPropProvider
					.getProperty("acl_not_created"));
		}
		prodFolderObj.save();

		return prodFolderObj.getObjectId();
	}

	@Override
	public IDfId createBulkProductFolder(IDfSysObject loFormReqNoObj,
			String strBulkProductName, IDfSession ingestSession,
			IDfId lifecycleId, IDfTypedObject productMetadataCollection)
			throws DfException, IngestException, IOException {

		IDfFolder loFoldrObj = (IDfFolder) ingestSession
				.getObject(loFormReqNoObj.getId("prod_dest"));
		String strlsProdDestFldrPath = loFoldrObj.getRepeatingString(
				"r_folder_path", 0);
		if (!DctmUtility.isDctmObjectPresent(ingestSession,
				strlsProdDestFldrPath)) {
			LOGGER.error("Destination folder doesnt exist {}",
					strlsProdDestFldrPath);
			ingestLogProvider.appendFailedFilesMessage(String.format(
					messagesPropProvider.getProperty("prod_folder_missing"),
					strlsProdDestFldrPath));
			throw new IngestException(String.format(messagesPropProvider
					.getProperty("prod_folder_missing"), strlsProdDestFldrPath));
		}

		String fullProductFolderPath = strlsProdDestFldrPath + "/"
				+ strBulkProductName;
		LOGGER.info("fullProductFolderPath  {}", fullProductFolderPath);
		if (DctmUtility.isDctmObjectPresent(ingestSession,
				fullProductFolderPath)) {
			LOGGER.error("Product folder already exists {}",
					fullProductFolderPath);
			ingestLogProvider.appendFailedFilesMessage(String.format(
					messagesPropProvider.getProperty("prod_already_exists"),
					fullProductFolderPath));
			throw new IngestException(String.format(messagesPropProvider
					.getProperty("prod_already_exists"), fullProductFolderPath));
		}

		String buName = loFormReqNoObj.getString(IngestConstants.ATTR_BU_NAME);

		if (StringUtils.isEmpty(buName)) {
			LOGGER.error("Unable to resolve the Business Unit.");
			throw new IngestException(messagesPropProvider
					.getProperty("business_unit_empty"));
		}
		cachedDataProvider.populateExtensionMappingMap(buName, ingestSession);

		GenericPropertiesProvider prodTypeProp = new GenericPropertiesProviderImpl(
				IngestConstants.PRSN_PROD_TYPE_PROP_FILE);
		String productTypeFolder = prodTypeProp.getProperty(IngestUtility
				.getBUKey(buName).toUpperCase());
		IDfFolder prodFolderObj = (IDfFolder) ingestSession
				.newObject(productTypeFolder);
		prodFolderObj.setObjectName(strBulkProductName);
		prodFolderObj.setString("prsn_business", buName);
		prodFolderObj.setString("prsn_ps_product_type", loFormReqNoObj
				.getString(IngestConstants.ATTR_PRODUCT_TYPE));
		prodFolderObj.link(strlsProdDestFldrPath);

		String strPropertyFileName = cachedDataProvider.getPropertyFileName(
				IngestUtility.getBUKey(buName).toUpperCase(), true);

		if (strPropertyFileName.equalsIgnoreCase("NO MATCH")) {
			throw new IngestException(messagesPropProvider
					.getProperty("metadata_properties_file_missing"));
		} else {
			GenericPropertiesProvider prodMetaDataPropProvider = new GenericPropertiesProviderImpl(
					strPropertyFileName);
			Map<String, String> prodMetaDataProp = prodMetaDataPropProvider
					.getAllEntrySetAsArray();
			List<String> orderedKeysList = prodMetaDataPropProvider
					.getAllKeysAsOrderedList();

			addSysObjectAttributes(ingestSession, prodFolderObj,
					prodMetaDataProp, orderedKeysList,
					productMetadataCollection);
		}

		prodFolderObj.save();
		String folderId = prodFolderObj.getObjectId().getId();

		// Fetching the Product Folder just after Save() to do further
		// operations on Product.
		prodFolderObj = (IDfFolder) ingestSession.getObject(new DfId(folderId));

		prodFolderObj.attachPolicy(lifecycleId,
				IngestConstants.IN_PROGRESS_STATE, "");

		IDfACL ingestJobACLObj = DctmACLUtil.getProductFolderAcl(folderId,
				ingestJobConfigProvider.getAdminSession(),
				buName.toUpperCase(), loFormReqNoObj
						.getString("security_profile"));

		if (null != ingestJobACLObj) {
			prodFolderObj.setACL(ingestJobACLObj);
			prodFolderObj.setString("prsn_security_profile", loFormReqNoObj
					.getString("security_profile"));
		} else {
			ingestLogProvider.appendFailedFilesMessage(messagesPropProvider
					.getProperty("acl_not_created"));
			throw new IngestException(messagesPropProvider
					.getProperty("acl_not_created"));
		}
		prodFolderObj.save();

		return prodFolderObj.getObjectId();
	}

	/**
	 * private method to log the error and throw exception
	 * 
	 * @param loFormReqNoObj
	 *            object to update
	 * @param propKey
	 *            property key
	 * @param messageAttribute
	 * @throws IngestException
	 * @throws DfException
	 */
	private void logExceptionAndThrow(IDfSysObject loFormReqNoObj,
			String errorString) throws IngestException, DfException {
		loFormReqNoObj.setString(IngestConstants.ATTR_UPLOAD_STATUS,
				IngestConstants.REQ_STATUS_FAILED);
		loFormReqNoObj.save();

		ingestLogProvider.appendFailedFilesMessage(errorString);
		throw new IngestException(errorString);
	}

	/**
	 * Private method to create a relationship between products
	 * 
	 * @param folderId
	 *            Form Id
	 * @param linkattr
	 *            Attribute names used for linking
	 * @param linkvalue
	 *            Attribute values used for linking
	 * @param strbuName
	 *            Business Unit Name
	 * @param session
	 *            IDfSession of the ingest user
	 * @throws DfException
	 * @throws IngestException
	 */
	private void createDMRelation(String folderId, String linkattr,
			String linkvalue, String strbuName, IDfSession session)
			throws DfException, IngestException {

		IDfCollection objCollection = null;
		IDfCollection tmpCollection = null;

		try {
			DfClientX client = null;
			String formId = folderId;

			String attributeName = linkattr;
			String attributeValue = linkvalue;

			if (null == linkvalue || linkvalue.equals("")) {
				ingestLogProvider.appendFooterMessage(messagesPropProvider
						.getProperty("related_prod_notdefined"));
			} else {

				String[] repeatvalues = null;
				repeatvalues = attributeValue.split(",");

				String strBusinessUnit = strbuName;

				String parentid = folderId;

				for (int i = 0; i < repeatvalues.length; i++) {

					String dataQuery = "select r_object_id from prsn_ps_folder where "
							+ attributeName
							+ " = '"
							+ StringEscapeUtils.escapeSql(repeatvalues[i])
							+ "' and  prsn_business='"
							+ StringEscapeUtils.escapeSql(strBusinessUnit)
							+ "'";

					client = new DfClientX();
					IDfQuery qry = client.getQuery(); // Create query object
					qry.setDQL(dataQuery);
					LOGGER.info("Related dataQuery  " + dataQuery);

					objCollection = qry
							.execute(session, IDfQuery.DF_EXEC_QUERY);

					if (null == objCollection) {

						ingestLogProvider.appendFooterMessage(String.format(
								messagesPropProvider
										.getProperty("related_prod_missing"),
								repeatvalues[i]));
					} else {
						while (objCollection.next()) {

							for (int j = 0; j < objCollection.getAttrCount(); j++) {
								String chldId = objCollection.getValueAt(j)
										.toString();

								if (!formId.equals(chldId)) {
									String queryString = "create dm_relation object set parent_id='"
											+ parentid
											+ "' , set child_id='"
											+ chldId
											+ "' , set description = '"
											+ IngestConstants.PROD_RELATION_NAME
											+ "'";

									LOGGER.info("Create Relation queryString "
											+ queryString);
									IDfQuery qryrel = client.getQuery();
									qryrel.setDQL(queryString);
									tmpCollection = qryrel.execute(session,
											IDfQuery.DF_EXEC_QUERY);

									if (null != tmpCollection) {
										ingestLogProvider
												.appendFooterMessage(String
														.format(
																messagesPropProvider
																		.getProperty("prod_relation_created"),
																parentid,
																chldId));
									}

								} else {
									ingestLogProvider
											.appendFooterMessage(messagesPropProvider
													.getProperty("no_relate_toself"));
								}
							}
						}
					}
				}
			}
		} catch (DfException ex) {
			LOGGER.error("Error creating relationship ", ex);
			throw new IngestException(ex.getMessage(), ex);

		} finally {
			try {
				if (null != objCollection) {
					objCollection.close();
				}
				if (null != tmpCollection) {
					tmpCollection.close();
				}

			} catch (DfException e) {
				LOGGER.error(e.getMessage());
			}
		}
	}

	/**
	 * Method to fetch control vocab
	 * 
	 * @param session
	 *            Ingest Session
	 * @param strProdRef
	 *            Product Reference
	 * @return Mapped attribute name
	 * @throws IngestException
	 */
	public String getMappedAttrForProdRef(IDfSession session, String strProdRef)
			throws IngestException {
		String mappedFolderAttrName = "";
		try {
			IDfSysObject idfSysObjCV = (IDfSysObject) session
					.getObjectByQualification("prsn_ps_controlled_vocab where field_name='prod_ref'");

			String strRepFieldValue = idfSysObjCV.getAllRepeatingStrings(
					"field_value", ",");
			String strRepKeywords = idfSysObjCV.getAllRepeatingStrings(
					"keywords", ",");

			String[] strFieldValue = strRepFieldValue.split(",");
			String[] strKeywords = strRepKeywords.split(",");

			if (strFieldValue.length == strKeywords.length) {
				for (int i = 0; i < strFieldValue.length; i++) {

					if (strFieldValue[i].equals(strProdRef)) {
						mappedFolderAttrName = strKeywords[i];
						break;
					}
				}
			}
		} catch (DfException dfe) {
			throw new IngestException(dfe.getMessage(), dfe);
		}

		return mappedFolderAttrName;
	}
}
