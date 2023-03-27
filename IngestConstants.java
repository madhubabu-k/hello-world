package com.pearson.ps.ingest.main;

public final class IngestConstants {

	/**
	 * Private constructor to prevent creating instances
	 */
	private IngestConstants() {
	}

	public static final String MANDATORY_ATTRIBUTES_FILE = "MandatoryAttr.properties";

	public static final String PS_INGEST_JOB_CONFIG_PROP_FILE = "PSIngestJobConfig.properties";
	public static final String PRSN_INGEST_MAIL_PROP_FILE = "IngestMail.properties";
	public static final String PRSN_INGEST_MESSAGE_PROP_FILE = "IngestMessage.properties";
	public static final String PRSN_PROD_TYPE_PROP_FILE = "ProductTypes.properties";
	// public static final String PRSN_ASSET_META_INFO_PROP_FILE =
	// "AssetsMetaInfo.properties";
	public static final String PRSN_HIDDEN_FILES_LIST_PROP_FILE = "HiddenFilesList.properties";
	
	public static final String REQ_STATUS_PENDING = "PENDING";
	public static final String REQ_STATUS_INPROGRESS = "INPROGRESS";
	public static final String REQ_STATUS_FAILED = "Failed";

	public static final String INGEST_REQUEST_TYPE = "prsn_ps_form_product_info";
	public static final String INGEST_REQUEST_PREFIX = "ING REQ-";
	public static final String PROD_RELATION_NAME = "PRSN_PROD_RELATION";

	public static final String IN_PROGRESS_STATE = "0";

	/** The newline character to use for printing to the log. */
	public static final String NEWLINE = "\n";

	public static final String ATTR_MANIFEST_REQUIRED = "manifest_required";
	public static final String ATTR_UPLOAD_STATUS = "prd_upload_status";
	public static final String ATTR_BU_NAME = "bu_name"; // Modified in Release
															// 1.1

	/** The default object type for assets with no specific object type mapping */
	public static final String OBJECT_TYPE_DM_DOCUMENT = "dm_document";
	/** The object type for assets with no specific object type mapping */
	public static final String OBJECT_TYPE_PRSN_PS_CONTENT = "prsn_ps_content";
	/** The attribute used to store the contributor name of the product. */
	public static final String ATTR_CONTRIBUTOR_NAME = "prsn_contributor_name";
	/** The attribute used to store the contributor role of the product. */
	public static final String ATTR_CONTRIBUTOR_ROLE = "prsn_contributor_role";

	/** The object type for internal product folders. */
	public static final String OBJECT_TYPE_DM_FOLDER = "dm_folder";
	/** The object type for top-level business unit cabinets. */
	public static final String OBJECT_TYPE_PRSN_PS_CABINET = "prsn_ps_cabinet";
	/** The format used for setting time attributes in Documentum. */
	public static final String DEFAULT_TIME_FORMAT = "MM/dd/yyyy hh:mm:ss aa";
	/** The name of the product manifest. */
	public static final String PRODUCT_MANIFEST_FILENAME = "MANIFEST.xml";
	/** The message that ingest completed. */
	public static final String MESSAGE_INGEST_COMPLETE = "Ingest is complete.";
	/** The message that ingest did not completed. */
	public static final String MESSAGE_INGEST_DID_NOT_COMPLETE = "Ingest did not complete.";

	/** Added for Base Folder Object Type */

	public static final String PRSN_PS_FOLDER = "prsn_ps_folder";
	public static final String PRSN_PS_FOLDER_ATTR_PRSN_BUSINESS = "prsn_business";

	/**
	 * Added in Release 1.1
	 */
	public static final String INGEST_FORM_ATTR_IS_BULK_INGEST = "is_bulk_ingest_request";
	public static final String INGEST_FORM_ATTR_INGESTED_PRODUCTS_STATUS = "ingested_products_status";
	public static final String INGEST_FORM_ATTR_INGESTED_PRODUCTS_ID = "ingested_products_id";

	// Added Constants to get the attribute of EPM and B3 table for Product
	// Reference Value selected from Drop down.
	public static final String PROD_REF_ATTR_ISBN10 = "isbn10";
	public static final String PROD_REF_ATTR_ISBN13 = "isbn13";
	public static final String PROD_REF_ATTR_WORK_REF = "product_id";
	public static final String PROD_REF_ISBN10 = "ISBN10";
	public static final String PROD_REF_ISBN13 = "ISBN13";
	public static final String PROD_REF_WORK_REF = "Work Reference/Product ID";

	public static final String CONTENT_TRANSFER_STATUS_PROP_FILE = "TransferStatus.properties";
	public static final String CONTENT_TRANSFER_STATUS = "CONTENT_TRANSFER_STATUS";

	public static final String INGEST_FORM_ATTR_IS_ASSETS_INGEST = "is_assets_ingest_request";
	public static final String INGEST_FORM_ATTR_INGESTED_ASSETS_ID = "ingested_assets_id";
	public static final String ATTR_USER_BU_NAME = "user_business_unit";

	public static final String ATTR_PRODUCT_TYPE = "product_type";
	
	public static final String INGEST_FORM_ATTR_EXCLUDE_SYSTEM_FILES = "exclude_system_files";

	public static final String PRSN_PS_DK_FOLDER = "prsn_ps_dk_folder";
	public static final String PRSN_PS_PI_FOLDER = "prsn_ps_pi_folder";
	public static final String PRSN_PS_PENGUIN_FOLDER = "prsn_ps_penguin_folder";
}
