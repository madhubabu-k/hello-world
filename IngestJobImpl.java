/**
 * 
 */
package com.pearson.ps.ingest.main;

import java.io.File;
import java.io.FileInputStream;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.jms.JMSException;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import com.documentum.com.DfClientX;
import com.documentum.com.IDfClientX;
import com.documentum.fc.client.IDfACL;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfDocument;
import com.documentum.fc.client.IDfFolder;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSessionManager;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.client.IDfTypedObject;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.IDfId;
import com.documentum.fc.common.IDfTime;
import com.google.inject.Inject;
import com.pearson.ps.ingest.creator.AbstractIngestCreator;
import com.pearson.ps.ingest.creator.ProductFolderCreator;
import com.pearson.ps.ingest.exception.IngestException;
import com.pearson.ps.ingest.helper.ManifestHelper;
import com.pearson.ps.ingest.provider.CachedDataProvider;
import com.pearson.ps.ingest.provider.GenericPropertiesProvider;
import com.pearson.ps.ingest.provider.GenericPropertiesProviderImpl;
import com.pearson.ps.ingest.provider.IngestJobConfigProvider;
import com.pearson.ps.ingest.provider.IngestLogProvider;
import com.pearson.ps.ingest.provider.MailNotificationProvider;
import com.pearson.ps.ingest.utils.ActiveMQUtils;
import com.pearson.ps.ingest.utils.DctmUtility;
import com.pearson.ps.ingest.utils.IngestUtility;

/**
 * Ingest job implementation class. This class queries the docbase to check for
 * any jobs ready for processing It sends an email at the end of the processing
 * with the log file. It also sends a request to the business process server
 * with correlation id. This will be used to move the workflow to the next
 * activity.
 * 
 * @author Manav Leslie
 * 
 */
public class IngestJobImpl implements IngestJob {

	// Using SLF4J Logger facade
	private final Logger logger = LoggerFactory.getLogger(IngestJobImpl.class);

	private IngestJobConfigProvider ingestJobConfigProvider;
	private IngestLogProvider ingestLogProvider;
	private CachedDataProvider cachedDataProvider;
	private ProductFolderCreator productFolderCreator;
	private ManifestHelper manifestHelper;
	private MailNotificationProvider mailNotificationProvider;
	private GenericPropertiesProvider messagesPropProvider;

	// Executor framework for multi-threaded model
	private ExecutorService executor = null;
	private List<Future<String>> list;

	private static final String LIFECYCLE_QUALIFICATION = "dm_policy WHERE object_name = 'prsn_ps_content_lifecycle'";
	private static final int shortRightPadLength = 40;
	private static final int longRightPadLength = 70;

	// Following attributes are saved as private so that it can be accessed by
	// all thread instances running in the inner class
	private int ingestedFolders = 0;
	private int ingestedFiles = 0;
	private int failedFiles = 0;
	private int ingestedHiddenFiles = 0;
	private int totalHiddenFilesAtSource = 0;
	private int iReqCnt = 0;

	private static IDfId lifecycleId;

	private static IDfSessionManager ingestSessionMgr = null;
	private static IDfSession ingestSession = null;

	private boolean isManifestRequired = false;
	private String manifestXMLFilePath;
	// private IDfSysObject loFormReqNoObj;
	// private Map<String, String> extensionFormatMap;

	private String completeMessage = "Ingest Failed";
	private double allFilesContentSize = 0;
	private double hiddenFilesContentSize = 0;

	private String errorMessage = "";

	public String strSingleIngestProductName = "";

	private boolean isBulkIngestRequest = false;
	private boolean isAssetsIngestRequest = false;

	private boolean isExcludeSystemFiles = false;

	private String strBusinessUnit = "";
	private String strProdRef = "";
	private String strProductRefValue = "";
	private String strTableName = "";

	private ArrayList<String> existingAssetsList = new ArrayList<String>();
	private ArrayList<IDfId> successAssetsList = new ArrayList<IDfId>();
	private Set<String> setReqs = new HashSet<String>();

	// private Map<String, String> assetPropMap = null;
	// private List<String> orderedAttrsForAssetList = null;

	public IDfTypedObject metadataCollectionObject = null;
	private boolean isProductMetaDataFound = false;
	// private boolean isAssetMetaDataFound = false;

	private static final String DATE_FORMAT_STRING = "MM/dd/yyyy HH:mm:ss";
	private final SimpleDateFormat dateFormat = new SimpleDateFormat(
			DATE_FORMAT_STRING);

	private ArrayList<String> hiddenFilesList = new ArrayList<String>();

	/**
	 * Constructor class where the dependencies are being injected
	 * 
	 * @param ingestJobConfigProvider
	 *            User credentials provider
	 * @param ingestLogProvider
	 *            Ingest Log Provider
	 * @param cachedDataProvider
	 *            Cached Data provider
	 * @param productFolderCreator
	 *            Product Folder Creator class
	 * @param manifestHelper
	 *            Manifest helper class
	 * @param mailNotificationProvider
	 *            Mail Notification provider
	 */
	@Inject
	public IngestJobImpl(IngestJobConfigProvider ingestJobConfigProvider,
			IngestLogProvider ingestLogProvider,
			CachedDataProvider cachedDataProvider,
			ProductFolderCreator productFolderCreator,
			ManifestHelper manifestHelper,
			MailNotificationProvider mailNotificationProvider,
			GenericPropertiesProvider messagesPropProvider) {
		this.ingestJobConfigProvider = ingestJobConfigProvider;
		this.ingestLogProvider = ingestLogProvider;
		this.cachedDataProvider = cachedDataProvider;
		this.productFolderCreator = productFolderCreator;
		this.manifestHelper = manifestHelper;
		this.mailNotificationProvider = mailNotificationProvider;
		this.messagesPropProvider = messagesPropProvider;
		list = new ArrayList<Future<String>>();
	}

	/*
	 * 
	 * @see com.pearson.ps.ingest.main.IIngestJob#startIngest()
	 */
	@Override
	public void startIngest() {

		logger.info("Starting startIngest()....");

		String ingestRequestId = "";

		try {
			ingestRequestId = ActiveMQUtils.browseMessageFromQueue(
					ingestJobConfigProvider.getIngestActiveMQueueName(),
					this.ingestJobConfigProvider.getActiveMQueueURL());
			logger.info("Browsed Ingest Request Id :: {}", ingestRequestId);

			if (!StringUtils.isEmpty(ingestRequestId)) {
				setReqs.add(ingestRequestId);
				iReqCnt++;
				if (iReqCnt == setReqs.size()) {
					ingestRequestId = ActiveMQUtils
							.retrieveMessageFromQueue(ingestJobConfigProvider
									.getIngestActiveMQueueName(),
									this.ingestJobConfigProvider
											.getActiveMQueueURL());

					logger.info("Ingest Request Id :: {}", ingestRequestId);

					if (!StringUtils.isEmpty(ingestRequestId)) {

						IDfCollection loSelectCol = null;

						String strlsQueryUpdate = "";

						String strlsQuerySelect = "select correlation_id, workflow_id, last_access_date, valid_date from dm_sysobject dms, dm_dbo.IngestRequest_Status irs where dms.r_object_id=irs.formreqno and status='"
								+ IngestConstants.REQ_STATUS_PENDING
								+ "' and formreqno = '" + ingestRequestId + "'";

						logger.info(
								"New StrlsQuerySelect in startIngest() : {}",
								strlsQuerySelect);
						try {
							loSelectCol = DctmUtility.execSelectQuery(
									strlsQuerySelect, ingestJobConfigProvider
											.getAdminSession());
							while (loSelectCol.next()) {
								String strasCorrID = loSelectCol
										.getString("correlation_id");
								String strasWrkflowID = loSelectCol
										.getString("workflow_id");

								Calendar todayDateCal = Calendar.getInstance();

								String strUpdatedLastAccessDate = dateFormat
										.format(todayDateCal.getTime());

								Date lastAccessDate = dateFormat
										.parse(strUpdatedLastAccessDate);
								Date validDate = null;

								IDfTime validAccessDate = loSelectCol
										.getTime("valid_date");
								if (validAccessDate.isNullDate()) {
									strlsQueryUpdate = "update dm_dbo.IngestRequest_Status set status='"
											+ IngestConstants.REQ_STATUS_INPROGRESS
											+ "' , last_access_date = DATE('"
											+ strUpdatedLastAccessDate
											+ "', '"
											+ DATE_FORMAT_STRING
											+ "') where correlation_id='"
											+ strasCorrID
											+ "' and workflow_id='"
											+ strasWrkflowID
											+ "' and formreqno='"
											+ ingestRequestId + "'";
								} else {
									String strasValidDate = validAccessDate
											.asString(DATE_FORMAT_STRING);
									validDate = dateFormat
											.parse(strasValidDate);

									strlsQueryUpdate = "update dm_dbo.IngestRequest_Status set status='"
											+ IngestConstants.REQ_STATUS_INPROGRESS
											+ "' , last_access_date = DATE('"
											+ strUpdatedLastAccessDate
											+ "', '"
											+ DATE_FORMAT_STRING
											+ "') where correlation_id='"
											+ strasCorrID
											+ "' and workflow_id='"
											+ strasWrkflowID
											+ "' and formreqno='"
											+ ingestRequestId + "'";
								}

								logger
										.info(
												"strlsQueryUpdate in startIngest() : {}",
												strlsQueryUpdate);
								DctmUtility.execUpdateQuery(strlsQueryUpdate,
										ingestJobConfigProvider
												.getAdminSession());

								ingestSelectedProduct(ingestRequestId,
										strasCorrID, strasWrkflowID,
										lastAccessDate, validDate);
							}

						} catch (Exception ex) {
							logger.error(ex.getMessage(), ex);
						} finally {
							try {
								if (null != loSelectCol
										&& loSelectCol.getState() != IDfCollection.DF_CLOSED_STATE) {
									loSelectCol.close();
								}
							} catch (DfException dfe) {
								logger.error(dfe.getMessage());
							}
						}
					}
				}
			}
			logger.info("Ending startIngest()....");
		} catch (JMSException jmse) {
			logger.error(jmse.getMessage(), jmse);
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		} finally {
			logger.info("Going to release Admin Session......");
			if (ingestJobConfigProvider.getAdminSession() != null
					&& ingestJobConfigProvider.getAdminSession().isConnected()) {
				ingestJobConfigProvider.getAdminSessionManager().release(
						ingestJobConfigProvider.getAdminSession());
				ingestJobConfigProvider.getAdminSessionManager()
						.flushSessions();
			}
			logger.info("Admin Session is released......");
		}
	}

	/**
	 * Private method which will be called to ingest a selected product
	 * 
	 * @param formObjectId
	 *            ObjectId of the ingest form
	 * @param correlationId
	 *            Correlation id for the workflow instance
	 */
	public void ingestSelectedProduct(String formObjectId,
			String correlationId, String workflowId, Date lastAccessDate,
			Date validDate) {
		logger.info("Starting ingest for : {}", ingestJobConfigProvider
				.getAdminUserName());
		logger.info("Starting ingest for with values : {} {} ", formObjectId,
				correlationId);

		try {
			IDfSysObject loFormReqNoObj = (IDfSysObject) ingestJobConfigProvider
					.getAdminSession().getObject(new DfId(formObjectId));

			isBulkIngestRequest = loFormReqNoObj
					.getBoolean(IngestConstants.INGEST_FORM_ATTR_IS_BULK_INGEST);

			isAssetsIngestRequest = loFormReqNoObj
					.getBoolean(IngestConstants.INGEST_FORM_ATTR_IS_ASSETS_INGEST);

			isExcludeSystemFiles = loFormReqNoObj
					.getBoolean(IngestConstants.INGEST_FORM_ATTR_EXCLUDE_SYSTEM_FILES);

			/**
			 * Remove all values of Product/Assets Information from Ingest Form.
			 * This is required when the Ingest Request would be Re-run.
			 */

			if (isAssetsIngestRequest) {
				loFormReqNoObj
						.removeAll(IngestConstants.INGEST_FORM_ATTR_INGESTED_ASSETS_ID);

			} else {
				loFormReqNoObj
						.removeAll(IngestConstants.INGEST_FORM_ATTR_INGESTED_PRODUCTS_ID);
				loFormReqNoObj
						.removeAll(IngestConstants.INGEST_FORM_ATTR_INGESTED_PRODUCTS_STATUS);
			}
			loFormReqNoObj.save();
			loFormReqNoObj = (IDfSysObject) ingestJobConfigProvider
					.getAdminSession().getObject(loFormReqNoObj.getObjectId());

			/** End */

			if (isAssetsIngestRequest) {
				if (validDate != null) {
					if (lastAccessDate.after(validDate)) {
						errorMessage = String.format(messagesPropProvider
								.getProperty("max_pool_days_reached"));

						updateIngestStatusFailed(formObjectId, correlationId,
								workflowId, lastAccessDate, validDate);

						String productNameWithPath = loFormReqNoObj
								.getString("product_name");

						productNameWithPath = productNameWithPath.replaceAll(
								"\\\\", "/");

						IDfFolder loFoldrObj = (IDfFolder) ingestJobConfigProvider
								.getAdminSession().getObject(
										loFormReqNoObj.getId("prod_dest"));
						String strAssetDestFldrPath = loFoldrObj
								.getRepeatingString("r_folder_path", 0);

						ingestLogProvider
								.appendMainMessage("################### ASSETS INGEST REPORT ###################");
						ingestLogProvider.appendMainMessage("\n");
						ingestLogProvider.appendMainMessage(IngestUtility
								.getRightPaddedString("Ingest Request Number",
										loFormReqNoObj.getObjectName(),
										shortRightPadLength));
						ingestLogProvider.appendMainMessage(IngestUtility
								.getRightPaddedString(
										"Assets Ingesting into Destination",
										strAssetDestFldrPath,
										shortRightPadLength));
						ingestLogProvider.appendMainMessage(IngestUtility
								.getRightPaddedString("Initiators Name",
										loFormReqNoObj.getString("owner_name"),
										shortRightPadLength));

						String srcFolderPath = loFormReqNoObj
								.getString("source_folder")
								+ "/" + productNameWithPath;
						ingestLogProvider.appendMainMessage(IngestUtility
								.getRightPaddedString("Source Location",
										srcFolderPath, shortRightPadLength));
						ingestLogProvider
								.appendMainMessage(IngestUtility
										.getRightPaddedString(
												"Date/Time of Initiation",
												IngestUtility
														.getDateAsFormattedString(IngestConstants.DEFAULT_TIME_FORMAT),
												shortRightPadLength));

						finishLogAndMail(formObjectId, correlationId);

						loFormReqNoObj.setString(
								IngestConstants.ATTR_UPLOAD_STATUS,
								IngestConstants.REQ_STATUS_FAILED);
						loFormReqNoObj.save();
					} else {
						String productNameWithPath = loFormReqNoObj
								.getString("product_name");

						productNameWithPath = productNameWithPath.replaceAll(
								"\\\\", "/");

						String srcFolderPath = loFormReqNoObj
								.getString("source_folder")
								+ "/" + productNameWithPath;

						boolean isSourceFolderExist = new File(srcFolderPath)
								.exists();

						if (isSourceFolderExist) {

							String transferStatusFilePath = srcFolderPath
									+ "/"
									+ IngestConstants.CONTENT_TRANSFER_STATUS_PROP_FILE;
							boolean isTransferCompleted = isContentTransferCompleted(transferStatusFilePath);

							if (isTransferCompleted) {
								processAssetsIngest(formObjectId, correlationId);
							} else {
								updateIngestStatusPending(formObjectId,
										correlationId, workflowId,
										lastAccessDate, validDate);
							}
						} else {
							updateIngestStatusPending(formObjectId,
									correlationId, workflowId, lastAccessDate,
									validDate);
						}
					}
				} else {
					processAssetsIngest(formObjectId, correlationId);
				}
			} else if (isBulkIngestRequest) {
				if (validDate != null) {
					if (lastAccessDate.after(validDate)) {
						errorMessage = String.format(messagesPropProvider
								.getProperty("max_pool_days_reached"));

						String sourceBulkFolderNameWithPath = loFormReqNoObj
								.getString("source_bulk_folder_name");

						sourceBulkFolderNameWithPath = sourceBulkFolderNameWithPath
								.replaceAll("\\\\", "/");

						/*
						 * loFormReqNoObj.setString("product_name",
						 * sourceBulkFolderName); loFormReqNoObj.save();
						 */

						updateIngestStatusFailed(formObjectId, correlationId,
								workflowId, lastAccessDate, validDate);

						ingestLogProvider
								.appendMainMessage("################### BULK PRODUCTS INGEST REPORT ###################");
						ingestLogProvider.appendMainMessage("\n");
						ingestLogProvider.appendMainMessage(IngestUtility
								.getRightPaddedString("Ingest Request Number",
										loFormReqNoObj.getObjectName(),
										shortRightPadLength));
						ingestLogProvider.appendMainMessage(IngestUtility
								.getRightPaddedString("Initiators Name",
										loFormReqNoObj.getString("owner_name"),
										shortRightPadLength));

						String srcFolderPath = loFormReqNoObj
								.getString("source_folder")
								+ "/" + sourceBulkFolderNameWithPath;

						ingestLogProvider.appendMainMessage(IngestUtility
								.getRightPaddedString("Source Location",
										srcFolderPath, shortRightPadLength));
						ingestLogProvider
								.appendMainMessage(IngestUtility
										.getRightPaddedString(
												"Date/Time of Initiation",
												IngestUtility
														.getDateAsFormattedString(IngestConstants.DEFAULT_TIME_FORMAT),
												shortRightPadLength));

						ingestLogProvider
								.appendMainMessage(IngestUtility
										.getRightPaddedString(
												"Date/Time of Completion",
												IngestUtility
														.getDateAsFormattedString(IngestConstants.DEFAULT_TIME_FORMAT),
												shortRightPadLength));

						ingestLogProvider.appendMainMessage(IngestUtility
								.getRightPaddedString("Job Status",
										completeMessage, shortRightPadLength));

						ingestLogProvider
								.appendMainMessage(IngestUtility
										.getRightPaddedString(
												"No. of Successful Ingested Products ",
												String.valueOf(0),
												shortRightPadLength));
						ingestLogProvider
								.appendMainMessage(IngestUtility
										.getRightPaddedString(
												"No. of Failed Ingested Products ",
												String.valueOf(0),
												shortRightPadLength));
						ingestLogProvider.appendMainMessage(IngestUtility
								.getRightPaddedString("Failed Products Name ",
										"", shortRightPadLength));
						ingestLogProvider.appendMainMessage("\n");

						if (!errorMessage.equals("")) {
							ingestLogProvider
									.appendFooterMessage("################# ERROR / WARNING MESSAGES ###############");
							ingestLogProvider.appendFooterMessage(errorMessage);
						}
						ingestLogProvider
								.appendFooterMessage("########################## END ##########################");

						this.mailNotificationProvider.mailLogFile(formObjectId,
								ingestLogProvider
										.returnConsolidateBulkProductLog());
						logger
								.info("**********************************************************");
						logger.info("Mail Sent to Archivist ");
						logger
								.info("**********************************************************");
						logger.info(ingestLogProvider
								.returnConsolidateBulkProductLog().toString());
						logger
								.info("**********************************************************");

						ingestLogProvider.clearBuffers();

						String bpsURL = ingestJobConfigProvider.getInBoundURL()
								+ correlationId;
						IngestUtility.connectAsHTTPGet(bpsURL);

						loFormReqNoObj.setString(
								IngestConstants.ATTR_UPLOAD_STATUS,
								IngestConstants.REQ_STATUS_FAILED);
						loFormReqNoObj.save();
					} else {
						String sourceBulkFolderNameWithPath = loFormReqNoObj
								.getString("source_bulk_folder_name");

						sourceBulkFolderNameWithPath = sourceBulkFolderNameWithPath
								.replaceAll("\\\\", "/");

						String srcFolderPath = loFormReqNoObj
								.getString("source_folder")
								+ "/" + sourceBulkFolderNameWithPath;

						boolean isSourceFolderExist = new File(srcFolderPath)
								.exists();

						if (isSourceFolderExist) {

							String transferStatusFilePath = srcFolderPath
									+ "/"
									+ IngestConstants.CONTENT_TRANSFER_STATUS_PROP_FILE;
							boolean isTransferCompleted = isContentTransferCompleted(transferStatusFilePath);

							if (isTransferCompleted) {
								processBulkIngest(formObjectId, correlationId);
							} else {
								updateIngestStatusPending(formObjectId,
										correlationId, workflowId,
										lastAccessDate, validDate);
							}
						} else {
							updateIngestStatusPending(formObjectId,
									correlationId, workflowId, lastAccessDate,
									validDate);
						}
					}
				} else {
					processBulkIngest(formObjectId, correlationId);
				}
			} else {
				if (validDate != null) {
					if (lastAccessDate.after(validDate)) {
						errorMessage = String.format(messagesPropProvider
								.getProperty("max_pool_days_reached"));

						updateIngestStatusFailed(formObjectId, correlationId,
								workflowId, lastAccessDate, validDate);

						String productNameWithPath = loFormReqNoObj
								.getString("product_name");

						productNameWithPath = productNameWithPath.replaceAll(
								"\\\\", "/");

						ingestLogProvider
								.appendMainMessage("################### PRODUCT INGEST REPORT ###################");
						ingestLogProvider.appendMainMessage("\n");
						ingestLogProvider.appendMainMessage(IngestUtility
								.getRightPaddedString("Ingest Request Number",
										loFormReqNoObj.getObjectName(),
										shortRightPadLength));

						String productName = "";
						if (productNameWithPath.lastIndexOf("/") != -1) {
							productName = productNameWithPath
									.substring(productNameWithPath
											.lastIndexOf("/") + 1);
						} else {
							productName = productNameWithPath;
						}

						ingestLogProvider.appendMainMessage(IngestUtility
								.getRightPaddedString("Ingesting Product",
										productName, shortRightPadLength));
						ingestLogProvider.appendMainMessage(IngestUtility
								.getRightPaddedString("Initiators Name",
										loFormReqNoObj.getString("owner_name"),
										shortRightPadLength));
						ingestLogProvider.appendMainMessage(IngestUtility
								.getRightPaddedString("Product Title",
										loFormReqNoObj.getString("prsn_title"),
										shortRightPadLength));

						String srcFolderPath = loFormReqNoObj
								.getString("source_folder")
								+ "/" + productNameWithPath;
						ingestLogProvider.appendMainMessage(IngestUtility
								.getRightPaddedString("Source Location",
										srcFolderPath, shortRightPadLength));
						ingestLogProvider
								.appendMainMessage(IngestUtility
										.getRightPaddedString(
												"Date/Time of Initiation",
												IngestUtility
														.getDateAsFormattedString(IngestConstants.DEFAULT_TIME_FORMAT),
												shortRightPadLength));

						finishLogAndMail(formObjectId, correlationId);

						loFormReqNoObj.setString(
								IngestConstants.ATTR_UPLOAD_STATUS,
								IngestConstants.REQ_STATUS_FAILED);
						loFormReqNoObj.save();
					} else {
						String productNameWithPath = loFormReqNoObj
								.getString("product_name");

						productNameWithPath = productNameWithPath.replaceAll(
								"\\\\", "/");

						String srcFolderPath = loFormReqNoObj
								.getString("source_folder")
								+ "/" + productNameWithPath;

						boolean isSourceFolderExist = new File(srcFolderPath)
								.exists();

						if (isSourceFolderExist) {

							String transferStatusFilePath = srcFolderPath
									+ "/"
									+ IngestConstants.CONTENT_TRANSFER_STATUS_PROP_FILE;
							boolean isTransferCompleted = isContentTransferCompleted(transferStatusFilePath);

							if (isTransferCompleted) {
								processSingleIngest(formObjectId, correlationId);
							} else {
								updateIngestStatusPending(formObjectId,
										correlationId, workflowId,
										lastAccessDate, validDate);
							}
						} else {
							updateIngestStatusPending(formObjectId,
									correlationId, workflowId, lastAccessDate,
									validDate);
						}
					}
				} else {
					processSingleIngest(formObjectId, correlationId);
				}
			}
		} catch (Exception e) {
			logger.error(e.getLocalizedMessage(), e);
		} finally {
			try {
				if (ingestSession != null && ingestSession.isConnected()) {
					ingestSessionMgr.release(ingestSession);
					ingestSessionMgr.flushSessions();
				}
			} catch (Exception e) {
				// Ignore error as session might have been released.
				logger.error(e.getMessage());
			}
		}
	}

	/**
	 * Method to read TransferStatus.properties file to check for the status of
	 * content transfer. It returns true if file is present with
	 * Content_Transfer_Status = true, false Otherwise.
	 * 
	 * @param transferStatusFilePath
	 * @return
	 * @throws Exception
	 */
	private boolean isContentTransferCompleted(String transferStatusFilePath) {
		boolean isTransferCompleted = false;
		File transferStatusFile = new File(transferStatusFilePath);
		boolean isTransferStatusFilePresent = transferStatusFile.exists();

		try {
			if (isTransferStatusFilePresent) {
				java.util.Properties properties = new java.util.Properties();
				properties.load(new FileInputStream(transferStatusFile));
				String contentTransferStatus = properties
						.getProperty(IngestConstants.CONTENT_TRANSFER_STATUS);

				if (contentTransferStatus.equalsIgnoreCase("true")) {
					isTransferCompleted = true;
				}
			}
		} catch (Exception ie) {
			logger.error(ie.getLocalizedMessage(), ie);
		}
		return isTransferCompleted;
	}

	/**
	 * Method to update the IngestRequest_Status Table with Failed status for
	 * the current Ingest Request.
	 * 
	 * @param formObjectId
	 * @param correlationId
	 * @param workflowId
	 * @param lastAccessDate
	 * @param validDate
	 * @throws DfException
	 */
	private void updateIngestStatusFailed(String formObjectId,
			String correlationId, String workflowId, Date lastAccessDate,
			Date validDate) throws DfException {
		// String strLastAccessDate =
		// dateFormat.format(lastAccessDate.getTime());

		// Calendar todayDateCal = Calendar.getInstance();
		// String strUpdatedLastAccessDate =
		// dateFormat.format(todayDateCal.getTime());
		// String strasValidDate = dateFormat.format(validDate.getTime());

		String updateIngestStatusFailedQuery = "update dm_dbo.IngestRequest_Status set status='"
				+ IngestConstants.REQ_STATUS_FAILED
				+ "'where correlation_id='"
				+ correlationId
				+ "' and workflow_id='"
				+ workflowId
				+ "' and formreqno='" + formObjectId + "'";

		logger.info("failedIngestStatusPendingQuery : {} ",
				updateIngestStatusFailedQuery);

		DctmUtility.execUpdateQuery(updateIngestStatusFailedQuery,
				ingestJobConfigProvider.getAdminSession());

	}

	/**
	 * Method to update the IngestRequest_Status Table with Pending status for
	 * the current Ingest Request so that it can be processed again.
	 * 
	 * @param formObjectId
	 * @param correlationId
	 * @param workflowId
	 * @param lastAccessDate
	 * @param validDate
	 * @throws DfException
	 * @throws JMSException
	 */
	private void updateIngestStatusPending(String formObjectId,
			String correlationId, String workflowId, Date lastAccessDate,
			Date validDate) throws DfException, JMSException {
		// String strLastAccessDate =
		// dateFormat.format(lastAccessDate.getTime());

		Calendar todayDateCal = Calendar.getInstance();
		todayDateCal.add(Calendar.SECOND, 1);
		String strUpdatedLastAccessDate = dateFormat.format(todayDateCal
				.getTime());
		// String strasValidDate = dateFormat.format(validDate.getTime());

		String updateIngestStatusPendingQuery = "update dm_dbo.IngestRequest_Status set status='"
				+ IngestConstants.REQ_STATUS_PENDING
				+ "' , last_access_date = DATE('"
				+ strUpdatedLastAccessDate
				+ "', '"
				+ DATE_FORMAT_STRING
				+ "') where correlation_id='"
				+ correlationId
				+ "' and workflow_id='"
				+ workflowId
				+ "' and formreqno='" + formObjectId + "'";

		logger.info("updateIngestStatusPendingQuery : {} ",
				updateIngestStatusPendingQuery);

		DctmUtility.execUpdateQuery(updateIngestStatusPendingQuery,
				ingestJobConfigProvider.getAdminSession());

		// Push Ingest task in the Active MQueue with message having
		// ingest form No.
		ActiveMQUtils.publishMessageToQueue(ingestJobConfigProvider
				.getIngestActiveMQueueName(), formObjectId,
				ingestJobConfigProvider.getActiveMQueueURL());

		// Go back to process the next request b'coz this request could not be
		// processed as it was a remote request whose content could not transfer
		// completely.
		startIngest();
	}

	/**
	 * 
	 * @param formObjectId
	 * @param correlationId
	 */
	private void processAssetsIngest(String formObjectId, String correlationId) {

		IDfSysObject loFormReqNoObj = null;
		try {
			loFormReqNoObj = (IDfSysObject) ingestJobConfigProvider
					.getAdminSession().getObject(new DfId(formObjectId));

			populateLifecycleId();

			String strAssetsIngestProdNameWithPath = loFormReqNoObj
					.getString("product_name");

			strAssetsIngestProdNameWithPath = strAssetsIngestProdNameWithPath
					.replaceAll("\\\\", "/");

			IDfFolder loFoldrObj = (IDfFolder) ingestJobConfigProvider
					.getAdminSession().getObject(
							loFormReqNoObj.getId("prod_dest"));
			String strAssetDestFldrPath = loFoldrObj.getRepeatingString(
					"r_folder_path", 0);

			ingestLogProvider
					.appendMainMessage("################### ASSETS INGEST REPORT ###################");
			ingestLogProvider.appendMainMessage("\n");
			ingestLogProvider
					.appendMainMessage(IngestUtility.getRightPaddedString(
							"Ingest Request Number", loFormReqNoObj
									.getObjectName(), shortRightPadLength));
			ingestLogProvider.appendMainMessage(IngestUtility
					.getRightPaddedString("Assets Ingesting into Destination",
							strAssetDestFldrPath, shortRightPadLength));
			ingestLogProvider.appendMainMessage(IngestUtility
					.getRightPaddedString("Initiators Name", loFormReqNoObj
							.getString("owner_name"), shortRightPadLength));

			String srcFolderPath = loFormReqNoObj.getString("source_folder")
					+ "/" + strAssetsIngestProdNameWithPath;
			ingestLogProvider.appendMainMessage(IngestUtility
					.getRightPaddedString("Source Location", srcFolderPath,
							shortRightPadLength));
			ingestLogProvider
					.appendMainMessage(IngestUtility
							.getRightPaddedString(
									"Date/Time of Initiation",
									IngestUtility
											.getDateAsFormattedString(IngestConstants.DEFAULT_TIME_FORMAT),
									shortRightPadLength));

			boolean isSourceFolderExist = new File(srcFolderPath).exists();

			if (isSourceFolderExist) {
				loFormReqNoObj.setString(IngestConstants.ATTR_UPLOAD_STATUS,
						"In Progress");
				loFormReqNoObj.save();

				loFormReqNoObj = (IDfSysObject) ingestJobConfigProvider
						.getAdminSession().getObject(new DfId(formObjectId));

				assignIngestSession(loFormReqNoObj);

				GenericPropertiesProvider hiddenFilesPropProvider = new GenericPropertiesProviderImpl(
						IngestConstants.PRSN_HIDDEN_FILES_LIST_PROP_FILE);
				hiddenFilesList = (ArrayList<String>) hiddenFilesPropProvider
						.getAllKeysAsOrderedList();

				cachedDataProvider.populateExtensionMappingMap(loFormReqNoObj
						.getString(IngestConstants.ATTR_USER_BU_NAME),
						ingestSession);

				executor = Executors.newFixedThreadPool(ingestJobConfigProvider
						.getIngestJobThreadCount());

				String strAssetDestFldrId = (loFormReqNoObj.getId("prod_dest"))
						.getId();
				String strAssetDestFldrPrdId = IngestUtility.getProductId(
						strAssetDestFldrId, ingestJobConfigProvider
								.getAdminSession());

				boolean isDestFldrPrd = false;

				if (strAssetDestFldrId.equals(strAssetDestFldrPrdId)) {
					isDestFldrPrd = true;
				}

				addFilesRecursive(new File(srcFolderPath),
						strAssetDestFldrPath, true, isDestFldrPrd);

				for (Future<String> future : list) {
					try {
						@SuppressWarnings("unused")
						// If needed this can be used for processing
						String returnValue = future.get();
						// logger.info("File ID {}", returnValue);
					} catch (Exception ex) {
						logger.error("Failed ", ex);

						Calendar todayDateCal = Calendar.getInstance();
						SimpleDateFormat dateFormat = new SimpleDateFormat(
								"yyyy-MM-dd HH:mm:ss");
						String strExceptionTime = dateFormat
								.format(todayDateCal.getTime());

						ingestLogProvider.appendFooterMessage(strExceptionTime
								+ " ERROR " + ex.getLocalizedMessage());

					}
				}
				executor.shutdown();
				completeMessage = (failedFiles > 0) ? "Ingest did not complete."
						: "Ingest completed.";

				// if (isAssetsIngestRequest) {
				loFormReqNoObj = (IDfSysObject) ingestJobConfigProvider
						.getAdminSession().getObject(new DfId(formObjectId));

				for (int i = 0; i < successAssetsList.size(); i++) {
					loFormReqNoObj
							.setRepeatingId(
									IngestConstants.INGEST_FORM_ATTR_INGESTED_ASSETS_ID,
									i, successAssetsList.get(i));
				}

				loFormReqNoObj.save();
				// }

			} else {
				logger.error("Product Source Folder Doesnt Exist : {}",
						srcFolderPath);
				ingestLogProvider.appendFailedFilesMessage(String.format(
						"Product Source Folder Doesnt Exist  '%s'",
						srcFolderPath));
				throw new IngestException(String.format(
						"Product Source Folder Doesnt Exist  '%s'",
						srcFolderPath));
			}
		} catch (Exception e) {
			logger.error("Error in ingestion... ", e);
			errorMessage = e.getLocalizedMessage();
			try {
				if (null != loFormReqNoObj) {
					loFormReqNoObj = (IDfSysObject) ingestJobConfigProvider
							.getAdminSession()
							.getObject(new DfId(formObjectId));
					loFormReqNoObj.setString(
							IngestConstants.ATTR_UPLOAD_STATUS,
							IngestConstants.REQ_STATUS_FAILED);
					loFormReqNoObj.save();
				}

			} catch (DfException e1) {
				logger.error("Error Saving the object with failed status ", e1);
			}
		}
		finishLogAndMail(formObjectId, correlationId);

	}

	/**
	 * Method to process the Ingest Request for Single Request.
	 * 
	 * @param formObjectId
	 * @param correlationId
	 */
	private void processSingleIngest(String formObjectId, String correlationId) {
		IDfSysObject loFormReqNoObj = null;
		try {
			loFormReqNoObj = (IDfSysObject) ingestJobConfigProvider
					.getAdminSession().getObject(new DfId(formObjectId));

			populateLifecycleId();

			String productNameWithPath = loFormReqNoObj
					.getString("product_name");

			productNameWithPath = productNameWithPath.replaceAll("\\\\", "/");

			ingestLogProvider
					.appendMainMessage("################### PRODUCT INGEST REPORT ###################");
			ingestLogProvider.appendMainMessage("\n");
			ingestLogProvider
					.appendMainMessage(IngestUtility.getRightPaddedString(
							"Ingest Request Number", loFormReqNoObj
									.getObjectName(), shortRightPadLength));

			strSingleIngestProductName = productNameWithPath
					.substring(productNameWithPath.lastIndexOf("/") + 1);

			ingestLogProvider.appendMainMessage(IngestUtility
					.getRightPaddedString("Ingesting Product",
							strSingleIngestProductName, shortRightPadLength));

			ingestLogProvider.appendMainMessage(IngestUtility
					.getRightPaddedString("Initiators Name", loFormReqNoObj
							.getString("owner_name"), shortRightPadLength));
			ingestLogProvider.appendMainMessage(IngestUtility
					.getRightPaddedString("Product Title", loFormReqNoObj
							.getString("prsn_title"), shortRightPadLength));

			String srcFolderPath = loFormReqNoObj.getString("source_folder")
					+ "/" + productNameWithPath;

			ingestLogProvider.appendMainMessage(IngestUtility
					.getRightPaddedString("Source Location", srcFolderPath,
							shortRightPadLength));
			ingestLogProvider
					.appendMainMessage(IngestUtility
							.getRightPaddedString(
									"Date/Time of Initiation",
									IngestUtility
											.getDateAsFormattedString(IngestConstants.DEFAULT_TIME_FORMAT),
									shortRightPadLength));

			boolean isSourceFolderExist = new File(srcFolderPath).exists();

			if (isSourceFolderExist) {
				loFormReqNoObj.setString(IngestConstants.ATTR_UPLOAD_STATUS,
						"In Progress");
				loFormReqNoObj.save();

				assignIngestSession(loFormReqNoObj);

				GenericPropertiesProvider hiddenFilesPropProvider = new GenericPropertiesProviderImpl(
						IngestConstants.PRSN_HIDDEN_FILES_LIST_PROP_FILE);
				hiddenFilesList = (ArrayList<String>) hiddenFilesPropProvider
						.getAllKeysAsOrderedList();

				// extensionFormatMap = DctmUtility.queryFormat(ingestSession);

				IDfFolder loFoldrObj = (IDfFolder) ingestSession
						.getObject(loFormReqNoObj.getId("prod_dest"));
				String strlsProdDestFldrPath = loFoldrObj.getRepeatingString(
						"r_folder_path", 0);
				String fullProductFolderPath = strlsProdDestFldrPath + "/"
						+ strSingleIngestProductName;

				IDfId productFolderId = productFolderCreator
						.createProductFolder(loFormReqNoObj, ingestSession,
								lifecycleId);

				manifestXMLFilePath = srcFolderPath + "/"
						+ IngestConstants.PRODUCT_MANIFEST_FILENAME;

				isManifestRequired = false;

				if (loFormReqNoObj.getString(
						IngestConstants.ATTR_MANIFEST_REQUIRED)
						.equalsIgnoreCase("T")) {
					isManifestRequired = true;
				}

				executor = Executors.newFixedThreadPool(ingestJobConfigProvider
						.getIngestJobThreadCount());

				addFilesRecursive(new File(srcFolderPath),
						fullProductFolderPath, true, true);

				for (Future<String> future : list) {
					try {
						@SuppressWarnings("unused")
						// If needed this can be used for processing
						String returnValue = future.get();
						// logger.info("File ID {}", returnValue);
					} catch (Exception ex) {
						logger.error("Failed ", ex);

						Calendar todayDateCal = Calendar.getInstance();
						SimpleDateFormat dateFormat = new SimpleDateFormat(
								"yyyy-MM-dd HH:mm:ss");
						String strExceptionTime = dateFormat
								.format(todayDateCal.getTime());

						ingestLogProvider.appendFooterMessage(strExceptionTime
								+ " ERROR " + ex.getLocalizedMessage());

					}
				}
				executor.shutdown();
				completeMessage = (failedFiles > 0) ? "Ingest did not complete."
						: "Ingest completed.";
				allFilesContentSize = Double.parseDouble(DctmUtility
						.getFolderContentsSize(ingestSession,
								fullProductFolderPath));

				String strProductIngestStatus = (failedFiles > 0) ? "Failed"
						: "Success";

				loFormReqNoObj = (IDfSysObject) ingestJobConfigProvider
						.getAdminSession().getObject(new DfId(formObjectId));
				loFormReqNoObj
						.setRepeatingString(
								IngestConstants.INGEST_FORM_ATTR_INGESTED_PRODUCTS_STATUS,
								0, strProductIngestStatus);
				loFormReqNoObj.setRepeatingId(
						IngestConstants.INGEST_FORM_ATTR_INGESTED_PRODUCTS_ID,
						0, productFolderId);
				loFormReqNoObj.save();

			} else {
				logger.error("Product Source Folder Doesnt Exist : {}",
						srcFolderPath);
				ingestLogProvider.appendFailedFilesMessage(String.format(
						"Product Source Folder Doesnt Exist  '%s'",
						srcFolderPath));
				throw new IngestException(String.format(
						"Product Source Folder Doesnt Exist  '%s'",
						srcFolderPath));
			}
		} catch (Exception e) {
			logger.error("Error ingesting product ", e);
			errorMessage = e.getLocalizedMessage();
			try {
				if (null != loFormReqNoObj) {
					loFormReqNoObj = (IDfSysObject) ingestJobConfigProvider
							.getAdminSession()
							.getObject(new DfId(formObjectId));
					loFormReqNoObj.setString(
							IngestConstants.ATTR_UPLOAD_STATUS,
							IngestConstants.REQ_STATUS_FAILED);
					loFormReqNoObj.save();
				}

			} catch (DfException e1) {
				logger.error("Error Saving the object with failed status ", e1);
			}
		}
		finishLogAndMail(formObjectId, correlationId);
	}

	/**
	 * Method which will be executed to Ingest Bulk Products for Bulk Ingest
	 * Request.
	 */
	private void processBulkIngest(String formObjectId, String correlationId) {
		int allProductCount = 0;
		int successProductCount = 0;
		int failedProductCount = 0;

		ArrayList<String> failedProductsList = new ArrayList<String>();

		IDfSysObject loFormReqNoObj = null;

		try {

			loFormReqNoObj = (IDfSysObject) ingestJobConfigProvider
					.getAdminSession().getObject(new DfId(formObjectId));

			ingestLogProvider
					.appendMainMessage("################### BULK PRODUCTS INGEST REPORT ###################");
			ingestLogProvider.appendMainMessage("\n");
			ingestLogProvider
					.appendMainMessage(IngestUtility.getRightPaddedString(
							"Ingest Request Number", loFormReqNoObj
									.getObjectName(), shortRightPadLength));
			ingestLogProvider.appendMainMessage(IngestUtility
					.getRightPaddedString("Initiators Name", loFormReqNoObj
							.getString("owner_name"), shortRightPadLength));

			String sourceBulkFolderNameWithPath = loFormReqNoObj
					.getString("source_bulk_folder_name");

			sourceBulkFolderNameWithPath = sourceBulkFolderNameWithPath
					.replaceAll("\\\\", "/");

			String srcFolderPath = loFormReqNoObj.getString("source_folder")
					+ "/" + sourceBulkFolderNameWithPath;

			ingestLogProvider.appendMainMessage(IngestUtility
					.getRightPaddedString("Source Location", srcFolderPath,
							shortRightPadLength));
			ingestLogProvider
					.appendMainMessage(IngestUtility
							.getRightPaddedString(
									"Date/Time of Initiation",
									IngestUtility
											.getDateAsFormattedString(IngestConstants.DEFAULT_TIME_FORMAT),
									shortRightPadLength));

			populateLifecycleId();

			/*
			 * loFormReqNoObj.setString("product_name", sourceBulkFolderName);
			 * loFormReqNoObj.save();
			 */

			// assignIngestSession();
			File sourceFolder = new File(srcFolderPath);
			boolean isSourceFolderExist = sourceFolder.exists();

			if (isSourceFolderExist) {

				loFormReqNoObj.setString(IngestConstants.ATTR_UPLOAD_STATUS,
						"In Progress");
				loFormReqNoObj.save();

				strBusinessUnit = loFormReqNoObj
						.getString(IngestConstants.ATTR_BU_NAME);
				if (StringUtils.isEmpty(strBusinessUnit)) {
					logger.error("Unable to resolve the Business Unit.");
					throw new IngestException(messagesPropProvider
							.getProperty("business_unit_empty"));
				}

				String strProductRefType = loFormReqNoObj.getString("prod_ref");
				strProdRef = getProductRefAttrName(strProductRefType);
				strTableName = getTableNameforMetadataLookup(strBusinessUnit);

				if (StringUtils.isEmpty(strProdRef)) {
					logger
							.error("Unable to resolve the Product Reference Attribute for the value Selected in Prod Ref Dropdown.");
					throw new IngestException(String.format(
							messagesPropProvider
									.getProperty("prod_ref_attr_not_found"),
							strProductRefType));
				}

				List<String> orderedAttrsForProductList = null;
				String strTableAttrsListForProduct = "";

				String strProductPropertyFileName = cachedDataProvider
						.getPropertyFileName(IngestUtility.getBUKey(
								strBusinessUnit).toUpperCase(), true);

				if (strProductPropertyFileName.equalsIgnoreCase("NO MATCH")) {
					logger
							.error("***** ERROR ***** Matching Properties File not found. The Product Metadata can not be mapped. The Ingest will not proceed.");
					throw new IngestException(messagesPropProvider
							.getProperty("metadata_properties_file_missing"));
				} else {
					GenericPropertiesProvider prodMetaDataPropProvider = new GenericPropertiesProviderImpl(
							strProductPropertyFileName);
					orderedAttrsForProductList = prodMetaDataPropProvider
							.getAllKeysAsOrderedList();

					for (String attrName : orderedAttrsForProductList) {
						/*
						 * if (strTableAttrsListForProduct.equals(""))
						 * strTableAttrsListForProduct = attrName; else
						 * strTableAttrsListForProduct =
						 * strTableAttrsListForProduct + "," + attrName;
						 */

						strTableAttrsListForProduct = strTableAttrsListForProduct
								+ attrName + ",";
					}
					strTableAttrsListForProduct = strTableAttrsListForProduct
							.substring(0, strTableAttrsListForProduct
									.lastIndexOf(","));
				}

				ArrayList<File> bulkProductFolders = new ArrayList<File>();

				File[] children = sourceFolder.listFiles();

				if (children != null) {
					for (File child : children) {
						if (child.isDirectory()) {
							bulkProductFolders.add(child);
						} else {
							if (child
									.getName()
									.equalsIgnoreCase(
											IngestConstants.CONTENT_TRANSFER_STATUS_PROP_FILE)) {
								logger
										.info(
												"This File might be used for Content Transfer Status, though it will not be ingested. File : {}",
												IngestConstants.CONTENT_TRANSFER_STATUS_PROP_FILE);
							} else {
								logger
										.error(
												"Bulk Product Source Folder should not contain file. This file would not be ingested. File : {}",
												child.getAbsolutePath());
							}
						}
					}
				} else {
					logger.error("Bulk Product Source Folder is Empty : {}",
							srcFolderPath);
					throw new IngestException(String.format(
							messagesPropProvider
									.getProperty("bulk_source_folder_empty"),
							srcFolderPath));
				}

				if (!bulkProductFolders.isEmpty()) {
					allProductCount = bulkProductFolders.size();

					IDfFolder loFoldrObj = (IDfFolder) ingestJobConfigProvider
							.getAdminSession().getObject(
									loFormReqNoObj.getId("prod_dest"));
					String strlsProdDestFldrPath = loFoldrObj
							.getRepeatingString("r_folder_path", 0);

					int productIndex = 0;
					boolean isProductIngestSuccess = false;

					assignIngestSession(loFormReqNoObj);

					GenericPropertiesProvider hiddenFilesPropProvider = new GenericPropertiesProviderImpl(
							IngestConstants.PRSN_HIDDEN_FILES_LIST_PROP_FILE);
					hiddenFilesList = (ArrayList<String>) hiddenFilesPropProvider
							.getAllKeysAsOrderedList();

					for (int count = 0; count < bulkProductFolders.size(); count++) {
						File productFolder = bulkProductFolders.get(count);

						ingestLogProvider
								.appendProductTempMessage("\n*************************************** PRODUCT INGEST REPORT ***************************************");
						ingestLogProvider
								.appendProductTempMessage(IngestUtility
										.getRightPaddedString(
												"Ingesting Product",
												productFolder.getName(),
												shortRightPadLength));
						ingestLogProvider
								.appendProductTempMessage(IngestUtility
										.getRightPaddedString(
												"Date/Time of Initiation",
												IngestUtility
														.getDateAsFormattedString(IngestConstants.DEFAULT_TIME_FORMAT),
												shortRightPadLength));

						String strBulkProductName = productFolder.getName();
						if (strBulkProductName.indexOf("_") > 0) {
							strProductRefValue = strBulkProductName.substring(
									0, strBulkProductName.indexOf("_"));
						} else {
							strProductRefValue = strBulkProductName;
						}
						if (!strProductRefValue.equals("")) {
							// assignIngestSession(loFormReqNoObj);

							isProductMetaDataFound = isMetadataFound(
									ingestSession, strTableName,
									strTableAttrsListForProduct, strProdRef,
									strProductRefValue);

							if (isProductMetaDataFound) {
								IDfId productFolderId = null;

								try {
									productFolderId = productFolderCreator
											.createBulkProductFolder(
													loFormReqNoObj,
													strBulkProductName,
													ingestSession, lifecycleId,
													metadataCollectionObject);

									if (null != productFolderId) {
										// extensionFormatMap =
										//DctmUtility.queryFormat(ingestSession)
										// ;

										metadataCollectionObject = null;

										String fullProductFolderPath = strlsProdDestFldrPath
												+ "/" + strBulkProductName;

										executor = Executors
												.newFixedThreadPool(ingestJobConfigProvider
														.getIngestJobThreadCount());

										String strBulkProdSourceFolderPath = srcFolderPath
												+ "/" + strBulkProductName;

										/*
										 * String strAssetPropertyFileName =
										 * cachedDataProvider
										 * .getPropertyFileName
										 * (IngestUtility.getBUKey
										 * (strBusinessUnit).toUpperCase(),
										 * true, false);
										 * 
										 * if (strAssetPropertyFileName.
										 * equalsIgnoreCase("NO MATCH")) {
										 * logger.error(
										 * "***** ERROR ***** Matching Properties File not found. The Asset Metadata can not be mapped."
										 * ); ingestLogProvider.
										 * appendFailedFilesMessage(
										 * "***** ERROR ***** Matching Properties File not found. The Asset Metadata can not be mapped."
										 * ); } else { GenericPropertiesProvider
										 * assetMetaDataPropProvider = new
										 * GenericPropertiesProviderImpl
										 * (strAssetPropertyFileName);
										 * assetPropMap =
										 * assetMetaDataPropProvider
										 * .getAllEntrySetAsArray();
										 * orderedAttrsForAssetList =
										 * assetMetaDataPropProvider
										 * .getAllKeysAsOrderedList();
										 * 
										 * String strTableAttrsListForAssets =
										 * ""; for(String attrName :
										 * orderedAttrsForAssetList) {
										 * if(strTableAttrsListForAssets
										 * .equals(""))
										 * strTableAttrsListForAssets =
										 * attrName; else
										 * strTableAttrsListForAssets =
										 * strTableAttrsListForAssets + "," +
										 * attrName; }
										 * strTableAttrsListForAssets =
										 * strTableAttrsListForAssets
										 * .substring(0,
										 * strTableAttrsListForAssets
										 * .lastIndexOf(","));
										 * 
										 * isAssetMetaDataFound =
										 * isMetadataFound(ingestSession,
										 * strTableName,
										 * strTableAttrsListForAssets,
										 * strProdRef, strProductRefValue);
										 * logger
										 * .info("Is Asset Metadata found : {}",
										 * isAssetMetaDataFound); }
										 */

										addFilesRecursive(new File(
												strBulkProdSourceFolderPath),
												fullProductFolderPath, true,
												true);

										for (Future<String> future : list) {
											try {
												@SuppressWarnings("unused")
												// If needed this can be used
												// for processing
												String returnValue = future
														.get();
												// logger.info("File ID {}",
												// returnValue);
											} catch (Exception ex) {
												logger.error("Failed ", ex);

												Calendar todayDateCal = Calendar
														.getInstance();
												SimpleDateFormat dateFormat = new SimpleDateFormat(
														"yyyy-MM-dd HH:mm:ss");
												String strExceptionTime = dateFormat
														.format(todayDateCal
																.getTime());

												ingestLogProvider
														.appendFooterMessage(strExceptionTime
																+ " ERROR "
																+ ex
																		.getLocalizedMessage());
											}
										}
										list.clear();
										executor.shutdown();
										metadataCollectionObject = null;

										String strProductIngestStatus = "";

										if (failedFiles > 0) {
											isProductIngestSuccess = false;
											failedProductCount++;

											failedProductsList
													.add(strBulkProductName);

											completeMessage = "Ingest did not complete.";
											strProductIngestStatus = "Failed";
										} else {
											isProductIngestSuccess = true;
											successProductCount++;

											completeMessage = "Ingest completed.";
											strProductIngestStatus = "Success";
										}
										allFilesContentSize = Double
												.parseDouble(DctmUtility
														.getFolderContentsSize(
																ingestSession,
																fullProductFolderPath));

										// String strProductIngestStatus =
										// (failedFiles > 0) ? "Failed" :
										// "Success";

										try {
											loFormReqNoObj = (IDfSysObject) ingestJobConfigProvider
													.getAdminSession()
													.getObject(
															new DfId(
																	formObjectId));

											loFormReqNoObj
													.setRepeatingString(
															IngestConstants.INGEST_FORM_ATTR_INGESTED_PRODUCTS_STATUS,
															productIndex,
															strProductIngestStatus);
											loFormReqNoObj
													.setRepeatingId(
															IngestConstants.INGEST_FORM_ATTR_INGESTED_PRODUCTS_ID,
															productIndex,
															productFolderId);
											loFormReqNoObj.save();

											productIndex++;
										} catch (Exception ex) {
											logger
													.error(
															"Error in saving Form Object.",
															ex);
										}
									}
								} catch (Exception ex) {
									isProductIngestSuccess = false;
									failedProductCount++;

									failedProductsList.add(strBulkProductName);

									logger
											.error(
													"Error ingesting bulk product ",
													ex);
									errorMessage = ex.getLocalizedMessage();
									/*
									 * try {
									 * loFormReqNoObj.setString(IngestConstants
									 * .ATTR_UPLOAD_STATUS,
									 * IngestConstants.REQ_STATUS_FAILED);
									 * loFormReqNoObj.save(); } catch
									 * (DfException e1) { logger.error(
									 * "Error Saving the object with failed status "
									 * , e1); }
									 */
								}
							} else {
								isProductIngestSuccess = false;
								failedProductCount++;

								failedProductsList.add(strBulkProductName);

								logger
										.error(
												"No metadata found for product reference value appended in Bulk Product Folder Name. This Folder will not be ingested. Folder : {}",
												productFolder.getAbsolutePath());
								ingestLogProvider
										.appendFooterMessage(String
												.format(
														messagesPropProvider
																.getProperty("metadata_missing_for_prod_ref_value"),
														productFolder
																.getAbsolutePath()));
							}
						} else {
							isProductIngestSuccess = false;
							failedProductCount++;

							failedProductsList.add(strBulkProductName);

							logger
									.error(
											"Bulk Product Folder doesnt have any Product Reference appended in its name. This Folder will not be ingested. Folder : {}",
											productFolder.getAbsolutePath());
							ingestLogProvider
									.appendFooterMessage(String
											.format(
													messagesPropProvider
															.getProperty("prod_ref_value_missing"),
													productFolder
															.getAbsolutePath()));
						}
						ingestLogProvider
								.appendFooterMessage("\n*************************************** End *******************************************");
						consolidateLogForBulkProduct(isProductIngestSuccess);

						// Set global variables to their default values.
						ingestedFiles = 0;
						ingestedFolders = 0;
						failedFiles = 0;
						ingestedHiddenFiles = 0;
						totalHiddenFilesAtSource = 0;
						allFilesContentSize = 0;
						hiddenFilesContentSize = 0;
						completeMessage = "Ingest Failed";
						errorMessage = "";
					}
				} else {
					logger
							.error(
									"Bulk Product Source Folder doesnt have any Product Folder to Ingest : {}",
									srcFolderPath);
					throw new IngestException(
							String
									.format(
											messagesPropProvider
													.getProperty("no_prod_in_bulk_source_folder"),
											srcFolderPath));
				}
			} else {
				logger.error("Bulk Product Source Folder Doesnt Exist : {}",
						srcFolderPath);
				throw new IngestException(String.format(messagesPropProvider
						.getProperty("bulk_source_folder_missing"),
						srcFolderPath));
			}
		} catch (Exception ex) {
			logger.error("Error ingesting bulk product ", ex);
			errorMessage = ex.getLocalizedMessage();
			try {
				if (null != loFormReqNoObj) {
					loFormReqNoObj = (IDfSysObject) ingestJobConfigProvider
							.getAdminSession()
							.getObject(new DfId(formObjectId));

					loFormReqNoObj.setString(
							IngestConstants.ATTR_UPLOAD_STATUS,
							IngestConstants.REQ_STATUS_FAILED);
					loFormReqNoObj.save();
				}
			} catch (DfException e1) {
				logger.error("Error Saving the object with failed status ", e1);
			}
		}

		if (allProductCount == failedProductCount) {
			completeMessage = "Ingest Failed.";
			try {
				loFormReqNoObj = (IDfSysObject) ingestJobConfigProvider
						.getAdminSession().getObject(new DfId(formObjectId));

				loFormReqNoObj.setString(IngestConstants.ATTR_UPLOAD_STATUS,
						IngestConstants.REQ_STATUS_FAILED);
				loFormReqNoObj.save();
			} catch (DfException e1) {
				logger.error("Error Saving the object with failed status ", e1);
			}
		} else if (allProductCount == successProductCount) {
			completeMessage = "Ingest Successful.";
		} else {
			completeMessage = "Ingest Partial Failed.";
		}

		ingestLogProvider
				.appendMainMessage(IngestUtility
						.getRightPaddedString(
								"Date/Time of Completion",
								IngestUtility
										.getDateAsFormattedString(IngestConstants.DEFAULT_TIME_FORMAT),
								shortRightPadLength));

		ingestLogProvider.appendMainMessage(IngestUtility.getRightPaddedString(
				"Job Status", completeMessage, shortRightPadLength));

		ingestLogProvider.appendMainMessage(IngestUtility.getRightPaddedString(
				"No. of Successful Ingested Products ", String
						.valueOf(successProductCount), shortRightPadLength));
		ingestLogProvider.appendMainMessage(IngestUtility.getRightPaddedString(
				"No. of Failed Ingested Products ", String
						.valueOf(failedProductCount), shortRightPadLength));

		if (failedProductCount > 0) {
			ingestLogProvider.appendMainMessage("\n");
			ingestLogProvider
					.appendMainMessage("################# LIST OF FAILED PRODUCTS ###############");

			for (int i = 0; i < failedProductsList.size(); i++) {
				ingestLogProvider.appendMainMessage(IngestUtility
						.getRightPaddedString(failedProductsList.get(i), "",
								longRightPadLength));
			}
			ingestLogProvider
					.appendMainMessage("##########################################################");
			ingestLogProvider.appendMainMessage("\n");
		} else {
			ingestLogProvider.appendMainMessage("\n");
		}

		if (!errorMessage.equals("")) {
			ingestLogProvider
					.appendFooterMessage("################# ERROR / WARNING MESSAGES ###############");
			ingestLogProvider.appendFooterMessage(errorMessage);
		}
		ingestLogProvider
				.appendFooterMessage("########################## END ##########################");

		try {
			this.mailNotificationProvider.mailLogFile(formObjectId,
					ingestLogProvider.returnConsolidateBulkProductLog());
			logger
					.info("**********************************************************");
			logger.info("Mail Sent to Archivist ");
			logger
					.info("**********************************************************");
			logger.info(ingestLogProvider.returnConsolidateBulkProductLog()
					.toString());
			logger
					.info("**********************************************************");
		} catch (Exception e) {
			logger.error("Error Sending mail ", e);
		}

		ingestLogProvider.clearBuffers();
		String bpsURL = ingestJobConfigProvider.getInBoundURL() + correlationId;
		IngestUtility.connectAsHTTPGet(bpsURL);
	}

	private void assignIngestSession(IDfSysObject loFormReqNoObj)
			throws DfException, IngestException {
		String archivistUserName = loFormReqNoObj.getRepeatingString(
				"wf_performer", 0);
		if (archivistUserName != null && !archivistUserName.equals("")) {
			logger.info("archivistUserName : {}", archivistUserName);
		} else {
			archivistUserName = loFormReqNoObj.getOwnerName();
			logger.info("archivistUserName not found. Using Owner name : {}",
					archivistUserName);
		}
		ingestSessionMgr = ingestJobConfigProvider
				.getArchivistSessionManager(archivistUserName);
		ingestSession = ingestSessionMgr.getSession(ingestJobConfigProvider
				.getDocbaseName());
	}

	/**
	 * This method consolidate the main log file for a Product in case of Bulk
	 * Ingest.
	 */
	private void consolidateLogForBulkProduct(boolean isProductIngestSuccess) {

		ingestLogProvider
				.appendProductTempMessage(IngestUtility
						.getRightPaddedString(
								"Date/Time of Completion",
								IngestUtility
										.getDateAsFormattedString(IngestConstants.DEFAULT_TIME_FORMAT),
								shortRightPadLength));
		ingestLogProvider.appendProductTempMessage(IngestUtility
				.getRightPaddedString("Job Status", completeMessage,
						shortRightPadLength));

		ingestLogProvider.appendProductTempMessage(IngestUtility
				.getRightPaddedString("Number of files Ingested", String
						.valueOf(ingestedFiles - ingestedHiddenFiles),
						shortRightPadLength));
		ingestLogProvider
				.appendProductTempMessage(IngestUtility.getRightPaddedString(
						"Number of hidden files", String
								.valueOf(totalHiddenFilesAtSource),
						shortRightPadLength));

		if (isExcludeSystemFiles) {
			ingestLogProvider
					.appendProductTempMessage(IngestUtility
							.getRightPaddedString(
									"Number of total files Ingested", String
											.valueOf(ingestedFiles
													- ingestedHiddenFiles),
									shortRightPadLength));
		} else {
			ingestLogProvider
					.appendProductTempMessage(IngestUtility
							.getRightPaddedString(
									"Number of total files Ingested",
									String
											.valueOf((ingestedFiles - ingestedHiddenFiles)
													+ totalHiddenFilesAtSource),
									shortRightPadLength));
		}

		ingestLogProvider.appendProductTempMessage(IngestUtility
				.getRightPaddedString("Number of folders Ingested", String
						.valueOf(ingestedFolders), shortRightPadLength));

		DecimalFormat df = new DecimalFormat("#.##");

		ingestLogProvider.appendProductTempMessage(StringUtils.rightPad(
				"Size of Ingest", shortRightPadLength)
				+ df
						.format(Double.parseDouble(df
								.format(allFilesContentSize))
								- Double.parseDouble(df
										.format(hiddenFilesContentSize)))
				+ "  MB");

		ingestLogProvider.appendProductTempMessage(StringUtils.rightPad(
				"Size of hidden files", shortRightPadLength)
				+ df.format(hiddenFilesContentSize) + "  MB");

		if (failedFiles > 0) {
			ingestLogProvider.appendProductTempMessage(IngestUtility
					.getRightPaddedString("Number of failed files", String
							.valueOf(failedFiles), shortRightPadLength));
			ingestLogProvider.appendProductTempMessage("\n");
			ingestLogProvider
					.appendProductTempMessage("################# LIST OF FAILED FILES / FOLDERS ###############");
			ingestLogProvider.appendProductTempMessage(IngestUtility
					.getRightPaddedString("FILE NAME", "FILE PATH",
							longRightPadLength));
			ingestLogProvider.appendFailedFilesMessageToTempBuffer();
		}
		ingestLogProvider
				.appendProductTempMessage("################# ERROR / WARNING MESSAGES ###############");
		ingestLogProvider.appendProductTempMessage(errorMessage);

		if (isProductIngestSuccess) {
			ingestLogProvider.appendSuccessProductMainMessage();
		} else {
			ingestLogProvider.appendFailedProductMainMessage();
		}
		ingestLogProvider.clearBuffers();
	}

	/**
	 * This method adds the footer and other information to the log and sends it
	 * to the archivist
	 * 
	 * @param formObjectId
	 *            Object Id of the form
	 * @param correlationId
	 *            correlation id of the workflow instance
	 */
	private void finishLogAndMail(String formObjectId, String correlationId) {
		ingestLogProvider
				.appendMainMessage(IngestUtility
						.getRightPaddedString(
								"Date/Time of Completion",
								IngestUtility
										.getDateAsFormattedString(IngestConstants.DEFAULT_TIME_FORMAT),
								shortRightPadLength));
		ingestLogProvider.appendMainMessage("\n");

		ingestLogProvider.appendMainMessage(IngestUtility.getRightPaddedString(
				"Job Status", completeMessage, shortRightPadLength));

		ingestLogProvider.appendMainMessage(IngestUtility.getRightPaddedString(
				"Number of files Ingested", String.valueOf(ingestedFiles
						- ingestedHiddenFiles), shortRightPadLength));
		ingestLogProvider
				.appendMainMessage(IngestUtility.getRightPaddedString(
						"Number of hidden files", String
								.valueOf(totalHiddenFilesAtSource),
						shortRightPadLength));

		if (isExcludeSystemFiles) {
			ingestLogProvider
					.appendMainMessage(IngestUtility.getRightPaddedString(
							"Number of total files Ingested", String
									.valueOf(ingestedFiles
											- ingestedHiddenFiles),
							shortRightPadLength));
		} else {
			ingestLogProvider
					.appendMainMessage(IngestUtility
							.getRightPaddedString(
									"Number of total files Ingested",
									String
											.valueOf((ingestedFiles - ingestedHiddenFiles)
													+ totalHiddenFilesAtSource),
									shortRightPadLength));
		}

		ingestLogProvider.appendMainMessage(IngestUtility.getRightPaddedString(
				"Number of folders Ingested", String.valueOf(ingestedFolders),
				shortRightPadLength));

		// if (!isAssetsIngestRequest) {
		DecimalFormat df = new DecimalFormat("#.##");

		ingestLogProvider.appendMainMessage(StringUtils.rightPad(
				"Size of Ingest", shortRightPadLength)
				+ df
						.format(Double.parseDouble(df
								.format(allFilesContentSize))
								- Double.parseDouble(df
										.format(hiddenFilesContentSize)))
				+ "  MB");
		// }

		ingestLogProvider.appendMainMessage(StringUtils.rightPad(
				"Size of hidden files", shortRightPadLength)
				+ df.format(hiddenFilesContentSize) + "  MB");

		if (failedFiles > 0) {
			ingestLogProvider.appendMainMessage(IngestUtility
					.getRightPaddedString("Number of failed files", String
							.valueOf(failedFiles), shortRightPadLength));
			ingestLogProvider.appendMainMessage("\n");
			ingestLogProvider
					.appendMainMessage("################# LIST OF FAILED FILES / FOLDERS ###############");
			ingestLogProvider.appendMainMessage(IngestUtility
					.getRightPaddedString("FILE NAME", "FILE PATH",
							longRightPadLength));
			ingestLogProvider.appendFailedFilesMessageToMain();
		}
		ingestLogProvider.appendMainMessage("\n\n");

		if (isAssetsIngestRequest) {
			if (existingAssetsList.size() > 0) {
				ingestLogProvider
						.appendMainMessage("################# LIST OF EXISTING ASSETS ###############");

				for (int i = 0; i < existingAssetsList.size(); i++) {
					ingestLogProvider.appendMainMessage(IngestUtility
							.getRightPaddedString(existingAssetsList.get(i),
									"", longRightPadLength));
				}
			}
		}

		ingestLogProvider.appendMainMessage("\n\n");
		ingestLogProvider
				.appendMainMessage("################# ERROR / WARNING MESSAGES ###############");
		ingestLogProvider.appendMainMessage(errorMessage);

		// Call the main notification provider to send the email
		// notification to indicate the
		// end of the form processing
		try {
			this.mailNotificationProvider.mailLogFile(formObjectId,
					ingestLogProvider.returnConsolidatedMessage());
			logger
					.info("**********************************************************");
			logger.info("Mail Sent to Archivist ");
			logger
					.info("**********************************************************");
			logger.info(ingestLogProvider.returnConsolidatedMessage()
					.toString());
			logger
					.info("**********************************************************");
		} catch (Exception e) {
			logger.error("Error Sending mail ", e);
		}

		String bpsURL = ingestJobConfigProvider.getInBoundURL() + correlationId;
		IngestUtility.connectAsHTTPGet(bpsURL);

	}

	/**
	 * Private method called to create a folder object
	 * 
	 * @param currFolder
	 *            Folder as file
	 * @param location
	 *            Location where the folder needs to be created
	 * @return Path of the folder created
	 * @throws DfException
	 */
	private String createFolder(File currFolder, String location) {

		String folderPath = location + "/" + currFolder.getName();

		try {
			if (!DctmUtility.isDctmObjectPresent(ingestJobConfigProvider
					.getAdminSession(), folderPath)) {
				IDfFolder folder = (IDfFolder) ingestJobConfigProvider
						.getAdminSession().newObject(
								IngestConstants.OBJECT_TYPE_DM_FOLDER);
				folder.setObjectName(currFolder.getName());

				folder.link(location);
				folder.save();

				if (isAssetsIngestRequest) {
					successAssetsList.add(folder.getObjectId());
				}

				folderPath = folder.getFolderPath(0);
				ingestedFolders++;
			} else {
				logger.warn("The dm_folder already present :: {} ", folderPath);

				if (isAssetsIngestRequest) {
					existingAssetsList.add(folderPath);
				}
			}
		} catch (DfException dfe) {
			logger.error(dfe.getLocalizedMessage(), dfe);

			ingestLogProvider.appendFailedFilesMessage(IngestUtility
					.getRightPaddedString(currFolder.getName(), folderPath,
							longRightPadLength));
		}

		return folderPath;

	}

	/**
	 * Private method used to recursively create files and folders
	 * 
	 * @param file
	 *            The current file/folder to be processed
	 * @param folderPath
	 *            Path of the current file/folder
	 * @param isProductFolder
	 *            Indicate whether the current folder is a product folder
	 * @throws DfException
	 */
	private void addFilesRecursive(File sourceFile, String destFolderPath,
			boolean isSourceProductFolder, boolean isDestPrdFldr)
			throws DfException {
		if (!isSourceProductFolder) {
			if (sourceFile.isDirectory()) {
				destFolderPath = createFolder(sourceFile, destFolderPath);

			} else {
				// Create new thread and pass it onto the executor framework
				Callable<String> worker = new IngestWorker(sourceFile,
						destFolderPath);
				Future<String> submit = executor.submit(worker);
				list.add(submit);
			}
		}

		final File[] children = sourceFile.listFiles();
		if (children != null) {
			for (File child : children) {

				if (isDestPrdFldr
						&& ((isSourceProductFolder && child
								.getName()
								.equalsIgnoreCase(
										IngestConstants.PRODUCT_MANIFEST_FILENAME)) || (isSourceProductFolder && child
								.getName()
								.equalsIgnoreCase(
										IngestConstants.CONTENT_TRANSFER_STATUS_PROP_FILE)))) {
					logger
							.info("Skipping the ingest of Manifest.XML/TransferStatus.properties file.");
					continue;
				}

				addFilesRecursive(child, destFolderPath, false, false);
			}
		}
	}

	/**
	 * Provate class to fetch the lifecycle id
	 * 
	 * @throws DfException
	 */
	private void populateLifecycleId() throws DfException {
		lifecycleId = ingestJobConfigProvider.getAdminSession()
				.getIdByQualification(LIFECYCLE_QUALIFICATION);
		logger.info("Lifecycle id : {}", lifecycleId.getId());
	}

	/**
	 * Private Inner class to ingest an asset It extends AbstractIngestCreator
	 * to share common code needed to create a product folder Implements
	 * Callable so that values can be returned by the thread
	 * 
	 */
	private class IngestWorker extends AbstractIngestCreator implements
			Callable<String> {

		private File currFile;
		private String currLocation;

		/**
		 * Constructor for the ingest file
		 * 
		 * @param file
		 *            File to be ingested
		 * @param location
		 *            Location of the file to be ingested
		 */
		public IngestWorker(File file, String location) {
			currFile = file;
			currLocation = location;
		}

		public String call() throws IngestException {

			boolean ingestFileFlag = true;

			String returnStr = "";
			IDfSession tempSession = null;

			String fileName = "";
			String filePath = "";

			int ingestedHiddenFilesCnt = 0;
			int ingestedFilesCnt = 0;
			int totalHiddenFilesCnt = 0;

			double hiddenFileContentSize = 0;
			double assetFileContentSize = 0;

			fileName = currFile.getName();
			filePath = currFile.getAbsolutePath();

			String assetFilePath = currLocation + "/" + fileName;

			try {
				if (hiddenFilesList != null && !hiddenFilesList.isEmpty()
						&& hiddenFilesList.contains(fileName)) {

					totalHiddenFilesCnt++;

					if (isExcludeSystemFiles) {
						ingestFileFlag = false;

						logger
								.warn(
										"The system file would be excluded from ingest :: {} ",
										assetFilePath);
					}
				}
			} finally {
				totalHiddenFilesAtSource = totalHiddenFilesAtSource
						+ totalHiddenFilesCnt;
			}

			if (!DctmUtility.isDctmObjectPresent(ingestJobConfigProvider
					.getAdminSession(), assetFilePath)) {

				if (ingestFileFlag) {

					try {

						// Flush any sessions not in use
						ingestSessionMgr.flushSessions();

						// Creating a new session for the thread
						tempSession = ingestSessionMgr
								.newSession(ingestJobConfigProvider
										.getDocbaseName());

						if (logger.isDebugEnabled()) {
							logger.debug(
									"Got DFC session for Asset Ingest...{}",
									tempSession);
						}

						String extension = null;
						int periodIndex = fileName.lastIndexOf('.');

						String objType = null;

						if (periodIndex != -1) {
							extension = fileName.substring(periodIndex);

							objType = cachedDataProvider
									.getObjTypeForExtension(extension);
						} else {
							objType = IngestConstants.OBJECT_TYPE_PRSN_PS_CONTENT;
							// extension = ".x";
						}

						IDfDocument document = (IDfDocument) tempSession
								.newObject(objType);
						document.setObjectName(fileName);

						/*
						 * String format = extensionFormatMap.get(extension); if
						 * (format == null) { format = "binary"; }
						 */

						String format = DctmUtility.getFileFormat(tempSession,
								filePath);
						// logger.info("File Format for file : '" +filePath+
						// "' is : '"
						// + format + "'");

						if (logger.isDebugEnabled()) {
							logger.debug("File to Ingest : {}", currFile
									.getName());
							logger.debug("File Location to Ingest : {}",
									currFile.getAbsolutePath());
							logger.debug("File Format for Ingest : {}", format);
						}

						document.setContentType(format);
						document.link(currLocation);
						document.setFile(filePath);
						document.save();
						returnStr = document.getObjectId().getId();

						// Fetching the asset object just after the save to do
						// further
						// operations on the asset.
						document = (IDfDocument) tempSession
								.getObject(new DfId(returnStr));

						if (logger.isDebugEnabled()) {
							logger.debug("Ingested File Id : {}", returnStr);
						}

						if (!isBulkIngestRequest && !isAssetsIngestRequest) {
							if (isManifestRequired) {

								Document tempManifestDocObject = manifestHelper
										.getManifestXMLDocObject(manifestXMLFilePath);

								String assetFullPath = filePath;
								String prodAssetPath = assetFullPath
										.substring(
												assetFullPath
														.indexOf(strSingleIngestProductName) - 1,
												assetFullPath.indexOf(fileName) - 1);

								NodeList listMetaDataFiles = manifestHelper
										.getMetaDataFileNodeList(tempManifestDocObject);

								if (listMetaDataFiles != null) {

									manifestHelper.setMetadataAttrsOnAssets(
											document, listMetaDataFiles,
											fileName, prodAssetPath);
									// Save the asset Object after setting the
									// metadata.
									document.save();
								} else {
									ingestLogProvider
											.appendFooterMessage("***Warning*** MetaData Files list is null.");
								}
							} /*
							 * else { GenericPropertiesProvider propProvider =
							 * new GenericPropertiesProviderImpl(
							 * IngestConstants.PRSN_ASSET_META_INFO_PROP_FILE);
							 * Map<String, String> propMap =
							 * propProvider.getAllEntrySetAsArray();
							 * 
							 * List<String> orderedKeysList =
							 * propProvider.getAllKeysAsOrderedList();
							 * 
							 * addSysObjectAttributes(tempSession, document,
							 * propMap, orderedKeysList, loFormReqNoObj); }
							 */
						}
						/*
						 * else { document.setString("prsn_business_unit",
						 * strBusinessUnit); addSysObjectAttributes(tempSession,
						 * document, assetPropMap, orderedAttrsForAssetList,
						 * metadataCollectionObject); }
						 */

						if (logger.isDebugEnabled()) {
							logger
									.debug("After setting metadata on Ingested Asset...");
							logger
									.debug("Before Attaching Lifecycle on Ingested Asset...");
						}

						document.attachPolicy(lifecycleId,
								IngestConstants.IN_PROGRESS_STATE, "");

						if (logger.isDebugEnabled()) {
							logger
									.debug("After Attaching Lifecycle on Ingested Asset...");
						}

						if (isAssetsIngestRequest) {
							successAssetsList.add(document.getObjectId());

							assetFileContentSize = Double.parseDouble(document
									.getString("r_content_size")) / 1024 / 1024;
						}

						ingestedFilesCnt++;

						if (hiddenFilesList != null
								&& !hiddenFilesList.isEmpty()
								&& hiddenFilesList.contains(fileName)) {
							document.setBoolean("a_is_hidden", true);
							document.save();

							ingestedHiddenFilesCnt++;

							hiddenFileContentSize = Double.parseDouble(document
									.getString("r_content_size")) / 1024 / 1024;

						}

						ingestSessionMgr.release(tempSession);

					} catch (Exception ie) {

						failedFiles++;

						ingestLogProvider
								.appendFailedFilesMessage(IngestUtility
										.getRightPaddedString(fileName,
												filePath, longRightPadLength));

						try {
							if (null != returnStr && !returnStr.equals("")) {
								IDfSysObject assetObj = (IDfSysObject) ingestJobConfigProvider
										.getAdminSession().getObject(
												new DfId(returnStr));

								if (assetObj != null) {
									if (assetObj.getPermit() != IDfACL.DF_PERMIT_DELETE) {
										assetObj.grant(ingestJobConfigProvider
												.getAdminUserName(),
												IDfACL.DF_PERMIT_DELETE, null);
										assetObj.save();
									}
									assetObj.destroy();
								}
							}

							ingestSessionMgr.release(tempSession);

						} catch (Exception ex) {
							logger.error(ex.getLocalizedMessage());
						}
						logger.error("Error", ie);
						throw new IngestException(ie.getLocalizedMessage(),
								currFile);
					} finally {

						ingestedHiddenFiles = ingestedHiddenFiles
								+ ingestedHiddenFilesCnt;
						ingestedFiles = ingestedFiles + ingestedFilesCnt;

						hiddenFilesContentSize = hiddenFilesContentSize
								+ hiddenFileContentSize;

						if (isAssetsIngestRequest) {
							allFilesContentSize = allFilesContentSize
									+ assetFileContentSize;
						}

						try {
							if (tempSession != null
									&& tempSession.isConnected()) {
								ingestSessionMgr.release(tempSession);
							}
						} catch (Exception e) {
							// Ignore error as session might have been released.
							logger.error(e.getMessage());
						}
					}
				}

			} else {
				logger.warn("The asset file already present :: {} ",
						assetFilePath);

				if (isAssetsIngestRequest) {
					existingAssetsList.add(assetFilePath);
				}
			}

			return returnStr;
		}
	}

	/**
	 * Method to get the Table Name to lookup for meta data mapping on
	 * Product/Assets for Bulk Ingest.
	 * 
	 * @param buName
	 *            User's Business Unit.
	 * @return Table Name to lookup (EPM/B3)
	 */
	private String getTableNameforMetadataLookup(String buName) {
		String strTableName = "";

		if (buName.toUpperCase().startsWith("DK".toUpperCase())
				|| buName.toUpperCase().startsWith("Penguin".toUpperCase())) {
			strTableName = "dm_dbo.prsn_ps_b3";
		} else if (buName.toUpperCase().startsWith("PI".toUpperCase())) {
			strTableName = "dm_dbo.prsn_ps_epm";
		}

		return strTableName;
	}

	/**
	 * Method to get the Query to be executed for meta data lookup.
	 * 
	 * @param strTableName
	 *            Table Name to lookup.
	 * @param strTableAttrNameList
	 *            Table Attr Name List to lookup.
	 * @param strProdRef
	 *            Query qualification Attr name
	 * @param strProdRefValue
	 *            Query qualification Attr Value
	 * @return Query to be excuted.
	 */
	private String getMetadataQueryString(String strTableName,
			String strTableAttrNameList, String strProdRef,
			String strProdRefValue) {
		String strQuery = "SELECT " + strTableAttrNameList + " FROM "
				+ strTableName + " WHERE " + strProdRef + " = '"
				+ strProdRefValue + "' enable (return_top 1)";

		return strQuery;
	}

	/**
	 * Method that returns whether or not the meta-data returns for the
	 * Product/Assets. Also, this method populates the Collection with meta-data
	 * returned if any.
	 * 
	 * @param session
	 *            IDfSession for the Query to be executed.
	 * @param strTableName
	 *            Table Name to lookup.
	 * @param strTableAttrNameList
	 *            Table Attribute Name List to lookup.
	 * @param strProdRef
	 *            Query qualification Attribute name
	 * @param strProdRefValue
	 *            Query qualification Attribute Value
	 * @return whether the meta data returned or not.
	 */
	private boolean isMetadataFound(IDfSession session, String strTableName,
			String strTableAttrNameList, String strProdRef,
			String strProdRefValue) {
		boolean isMetadataFound = false;

		String strQuery = getMetadataQueryString(strTableName,
				strTableAttrNameList, strProdRef, strProdRefValue);
		logger.info("Query executed for metadata : {}", strQuery);

		IDfCollection metaDataCollection = null;

		try {
			IDfClientX cx = new DfClientX();
			IDfQuery query = cx.getQuery();
			query.setDQL(strQuery);

			metaDataCollection = query.execute(session, IDfQuery.EXEC_QUERY);

			if (null != metaDataCollection) {
				while (metaDataCollection.next()) {
					isMetadataFound = true;
					metadataCollectionObject = metaDataCollection
							.getTypedObject();
				}
			}
		} catch (DfException dfe) {
			logger.error(dfe.getLocalizedMessage(), dfe);
			ingestLogProvider.appendFooterMessage(dfe.getLocalizedMessage());
		} finally {
			try {
				if (metaDataCollection != null) {
					metaDataCollection.close();
				}
			} catch (DfException dfe) {
				logger.error(dfe.getLocalizedMessage(), dfe);
			}
		}
		return isMetadataFound;
	}

	/**
	 * Method to get the qualification Attribute Name into meta-data lookup
	 * query for Product Ref Value selected from drop-down.
	 * 
	 * @param strProductRef
	 *            Product Reference Selected.
	 * @return Attribute Name corresponding to Product Reference.
	 */
	private String getProductRefAttrName(final String strProductRef) {

		String strProdRef = strProductRef;
		if (strProdRef.equals(IngestConstants.PROD_REF_ISBN10)) {
			strProdRef = IngestConstants.PROD_REF_ATTR_ISBN10;
		} else if (strProdRef.equals(IngestConstants.PROD_REF_ISBN13)) {
			strProdRef = IngestConstants.PROD_REF_ATTR_ISBN13;
		} else if (strProdRef.equals(IngestConstants.PROD_REF_WORK_REF)) {
			strProdRef = IngestConstants.PROD_REF_ATTR_WORK_REF;
		} else {
			strProdRef = "";
		}

		return strProdRef;
	}

}