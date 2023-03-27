/*****************************************************************************
 *
 *    File Name   : MailNotification.java  
 *    Purpose     : This class is used to send the mail to archivist with log file generated on Ingest.

 *    Created On  :     4th July,2011
 *    Created By  : HCL Technologies Ltd.
 *    Modified On :
 *    Modified By :     
 *    Changes Done:
 *    Remarks     :
 *
 *****************************************************************************/

package com.pearson.ps.ingest.provider;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

import javax.activation.DataHandler;
import javax.activation.FileDataSource;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

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
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import com.google.inject.Inject;
import com.pearson.ps.ingest.exception.IngestException;
import com.pearson.ps.ingest.main.IngestConstants;
import com.pearson.ps.ingest.main.InitiateIngestJob;

public class MailNotificationProviderImpl implements MailNotificationProvider {

	private IngestJobConfigProvider ingestJobConfigProvider;

	/**
	 * @param ingestJobConfigProvider
	 */
	@Inject
	public MailNotificationProviderImpl(
			IngestJobConfigProvider ingestJobConfigProvider) {
		this.ingestJobConfigProvider = ingestJobConfigProvider;
	}

	private String strmsFormNameforLogFile = "";
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MailNotificationProviderImpl.class);

	/*
	 * 
	 * @see
	 * com.pearson.ps.ingest.provider.MailNotificationProvider#mailLogFile(java
	 * .lang.String, java.lang.StringBuffer)
	 */
	@Override
	public void mailLogFile(String formReqNo, StringBuffer logFileStrBuffer)
			throws IngestException, DfException, IOException {

		String ingestLogFileName = "";

		IDfSession mailSession = this.ingestJobConfigProvider.getAdminSession();

		try {

			Properties props = new GenericPropertiesProviderImpl(
					IngestConstants.PRSN_INGEST_MAIL_PROP_FILE);
			// MailProperties props = MailProperties.getMailProperties();
            String msgText1="";
            String subject1="";
			String toAttrName = props.getProperty("to").trim();
			String from = props.getProperty("from").trim();
			String hostQuery = props.getProperty("host").trim();
			String msgText = props.getProperty("msgText").trim();
			String subject = props.getProperty("subject").trim();
			
			String bulkMsgText = props.getProperty("bulkMsgText").trim();
			String bulkSubject = props.getProperty("bulkSubject").trim();

			String filedir = props.getProperty("filedir").trim();
			String fileInitialName = props.getProperty("fileinitialname");
			String fileExtension = props.getProperty("fileextension");

			String to = getLastWFPerformerAddress(mailSession, toAttrName,
					formReqNo);

			IDfSysObject loFormSysobj = (IDfSysObject) mailSession
					.getObject(new DfId(formReqNo));
			strmsFormNameforLogFile = loFormSysobj.getObjectName().toString();
			
			if(!loFormSysobj.getBoolean("is_bulk_ingest_request"))
            {
				msgText1 = formatMail(msgText, loFormSysobj, mailSession,formReqNo);
				subject1 = formatMail(subject, loFormSysobj, mailSession, formReqNo);
            }
            else
            {
            	msgText1 = formatMail(bulkMsgText, loFormSysobj, mailSession,formReqNo);
            	subject1 = formatMail(bulkSubject, loFormSysobj, mailSession, formReqNo);
            }
			
			
			String host = getSMTPServer(mailSession, hostQuery);

			ingestLogFileName = getLogFileNameWithPath(logFileStrBuffer,
					filedir, fileInitialName, fileExtension);

			if (to != null && to.equals("")) {
				throw new IngestException("No Address '" + to
						+ "' is given for reciever to send the mail.");
			} else {
				// create some properties and get the default Session

				Properties props1 = System.getProperties();
				props1.put("mail.smtp.host", host);
				props1.put("mail.smtp.auth", false);
				// Set this property to send mails to valid Internet address otherwise no mail will be send to any valid recipient. 
				props1.put("mail.smtp.sendpartial", true);

				Session session = Session.getInstance(props1, null);
				session.setDebug(true);

				// create a message
				MimeMessage msg = new MimeMessage(session);
				msg.setFrom(new InternetAddress(from));

				InternetAddress[] address = { new InternetAddress(to) };
				msg.setRecipients(Message.RecipientType.TO, address);

				InternetAddress[] addressBCC = getBCCRecipients();
				if (addressBCC != null) {
					msg.setRecipients(Message.RecipientType.BCC, addressBCC);
				}

				msg.setSubject(subject1);

				// create and fill the first message part
				MimeBodyPart mbp1 = new MimeBodyPart();
				mbp1.setText(msgText1);

				// create the second message part
				MimeBodyPart mbp2 = new MimeBodyPart();

				// attach the file to the message
				FileDataSource fds = new FileDataSource(new File(
						ingestLogFileName));

				mbp2.setDataHandler(new DataHandler(fds));
				mbp2.setFileName(fds.getName());

				// create the Multipart and add its parts to it
				Multipart mp = new MimeMultipart();
				mp.addBodyPart(mbp1);
				mp.addBodyPart(mbp2);

				// add the Multipart to the message
				msg.setContent(mp);

				// set the Date: header
				msg.setSentDate(new Date());

				// send the message
				Transport.send(msg);
			}
		} catch (MessagingException mex) {
			LOGGER.error(mex.getMessage(), mex);
			@SuppressWarnings("unused")
			Exception ex = null;
			if ((ex = mex.getNextException()) != null) {

				LOGGER.error(mex.getMessage());
			}
		} finally {
			if (ingestLogFileName != null
					&& !ingestLogFileName.equalsIgnoreCase("")) {
				File ingestLogFile = new File(ingestLogFileName);

				if (ingestLogFile.exists()) {
					boolean isLogFileDeleted = ingestLogFile.delete();
					LOGGER.info("Log file delete status : {}", isLogFileDeleted);
				} else {
					LOGGER.info("Log file doesnt exist ");
				}
			}
		}
	}

	/**
	 * To Fetch BCC recipients from argument of main Ingest job.
	 * 
	 * @return InternetAddress[]
	 * @throws AddressException
	 */
	private InternetAddress[] getBCCRecipients() {
		String strBCC = InitiateIngestJob.bcc;
		String strSeperator = InitiateIngestJob.seperator;
		ArrayList<InternetAddress> addressBCCList = new ArrayList<InternetAddress>();

		if (strBCC != null && !strBCC.equals("")) {
			if (strSeperator == null || strSeperator.equals("")) {
				strSeperator = ";";
			}
			String[] strArrayBCC = strBCC.split(strSeperator);
			int iBCCcount = strArrayBCC.length;
			if (iBCCcount != 0) {
				for (int i = 0; i < iBCCcount; i++) {
					try {
						addressBCCList.add(new InternetAddress(strArrayBCC[i]));
					} catch (AddressException ade) {
						LOGGER.error(ade.getLocalizedMessage(), ade);
					}
				}
			}
		}

		// Now initializing addressBCC[] to remove any 'null' Internet Address
		// within addressBCC[]
		// which could cause NullPointer Exception while sending the e-mail.

		InternetAddress[] addressBCC = new InternetAddress[addressBCCList
				.size()];

		for (int i = 0; i < addressBCCList.size(); i++) {
			addressBCC[i] = addressBCCList.get(i);
		}

		return addressBCC;
	}

	/**
	 * Method to get the Last Workflow Performer Address who initates the
	 * Product Ingest Process to send the notification mail on its email
	 * address.
	 * 
	 * @param mailSession
	 * @param toAttrName
	 * @param formReqNo
	 * @return userAddress
	 * @throws DfException
	 */
	private String getLastWFPerformerAddress(IDfSession mailSession,
			String toAttrName, String formReqNo) throws DfException {

		String userAddress = "";
		IDfCollection wfPerformerColl = null;
		IDfCollection userAddressColl = null;
		try {
			String strWFQuery = "select " + toAttrName
					+ " from prsn_ps_form_product_info where r_object_id = '"
					+ formReqNo + "'";
			IDfQuery wfQuery = new DfQuery();
			wfQuery.setDQL(strWFQuery);

			LOGGER.info("strWFQuery : {}", strWFQuery);

			wfPerformerColl = wfQuery.execute(mailSession, DfQuery.READ_QUERY);

			String lastWFPerformer = "";

			while (wfPerformerColl.next()) {
				lastWFPerformer = wfPerformerColl.getAllRepeatingStrings(
						toAttrName, ";");
			}

			lastWFPerformer = lastWFPerformer.substring(lastWFPerformer
					.lastIndexOf(';') + 1, lastWFPerformer.length());

			if (lastWFPerformer != null && lastWFPerformer.equals("")) {
				IDfSysObject formObj = (IDfSysObject) mailSession
						.getObject(new DfId(formReqNo));
				if (null != formObj) {
					lastWFPerformer = formObj.getCreatorName();
				}
			}

			String strUserAddressQuery = "select user_address from dm_user where user_name = '"
					+ StringEscapeUtils.escapeSql(lastWFPerformer) + "'";
			LOGGER.info("strUserAddressQuery : {}", strUserAddressQuery);
			IDfQuery userAddressquery = new DfQuery();
			userAddressquery.setDQL(strUserAddressQuery);

			userAddressColl = userAddressquery.execute(mailSession,
					DfQuery.READ_QUERY);

			while (userAddressColl.next()) {
				userAddress = userAddressColl.getString("user_address");
			}

		} catch (DfException dfe) {
			LOGGER.error(dfe.getMessage(), dfe);
			throw dfe;
		} finally {
			try {
				if (null != wfPerformerColl && null != userAddressColl) {
					wfPerformerColl.close();
					userAddressColl.close();
				}
			} catch (DfException e) {
				LOGGER.error(e.getMessage());
			}
		}
		return userAddress;
	}

	/**
	 * Method to get the Last Workflow Performer Name who initates the Product
	 * Ingest Process to send the notification mail with his name.
	 * 
	 * @param mailSession
	 * @param toAttrName
	 * @param formReqNo
	 * @return userAddress
	 * @throws DfException
	 */

	private String getLastWFPerformerName(IDfSession mailSession,
			String toAttrName, String formReqNo) throws DfException {

		String lastWFPerformer = "";
		IDfCollection wfPerformerColl = null;

		try {
			String strWFQuery = "select " + toAttrName
					+ " from prsn_ps_form_product_info where r_object_id = '"
					+ formReqNo + "'";
			IDfQuery wfQuery = new DfQuery();
			wfQuery.setDQL(strWFQuery);

			LOGGER.info("strWFQuery : {}", strWFQuery);

			wfPerformerColl = wfQuery.execute(mailSession, DfQuery.READ_QUERY);

			while (wfPerformerColl.next()) {
				lastWFPerformer = wfPerformerColl.getAllRepeatingStrings(
						toAttrName, ";");
			}

			lastWFPerformer = lastWFPerformer.substring(lastWFPerformer
					.lastIndexOf(';') + 1, lastWFPerformer.length());

			if (lastWFPerformer != null && lastWFPerformer.equals("")) {
				IDfSysObject formObj = (IDfSysObject) mailSession
						.getObject(new DfId(formReqNo));
				if (null != formObj) {
					lastWFPerformer = formObj.getCreatorName();
				}
			}

		} catch (DfException dfe) {
			LOGGER.error(dfe.getMessage(), dfe);
			throw dfe;
		} finally {
			try {
				if (null != wfPerformerColl) {
					wfPerformerColl.close();

				}
			} catch (DfException e) {
				LOGGER.error(e.getMessage());
			}
		}
		return lastWFPerformer;
	}

	/**
	 * Method to create a Log File for this product ingest and return the Log
	 * File
	 * 
	 * @param filedir
	 * @param fileInitialName
	 * @param fileExtension
	 * @return ingestLogFileNameWithPath
	 * @throws IOException
	 * @throws IOException
	 */
	private String getLogFileNameWithPath(StringBuffer logFileStrBuffer,
			String filedir, String fileInitialName, String fileExtension)
			throws IOException {

		String ingestLogFileNameWithPath = "";
		// TODO get temp dir using System.getProperty("java.io.tmpdir")
		File fileDirs = new File(filedir);
		if (!fileDirs.exists()) {
			LOGGER
					.error("****ERROR****File Directory doesn't exist. Code is now creating the directory.");
			fileDirs.mkdirs();
		}

		Calendar calendar = Calendar.getInstance();

		SimpleDateFormat dateFormat = new SimpleDateFormat("dd MMM yyyy");
		SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");

		String strDateFormat = dateFormat.format(calendar.getTime());
		String strDate = strDateFormat.replace(" ", "");

		String strTimeFormat = timeFormat.format(calendar.getTime());
		String[] timeValues = strTimeFormat.split(":");
		String strTime = timeValues[0] + "h" + timeValues[1] + "m"
				+ timeValues[2] + "s";

		ingestLogFileNameWithPath = filedir + fileInitialName + "_"
				+ strmsFormNameforLogFile + "_" + strDate + "_" + strTime
				+ fileExtension;

		LOGGER.info("Ingest LogFile Name is : {}", ingestLogFileNameWithPath);

		File ingestLogFile = new File(ingestLogFileNameWithPath);

		if (!ingestLogFile.exists()) {

			ingestLogFile.createNewFile();

		}
		// Write the log data of String Buffer to
		// the Log File just created for this product ingest.
		writeLogDataToIngestLogFile(logFileStrBuffer, ingestLogFile);

		return ingestLogFileNameWithPath;
	}

	/**
	 * Method to read the data from String Buffer Created for Log File and write
	 * that data to Ingest Log file.
	 * 
	 * @param defaultLogFileInLog4j
	 *            String path of input file from which the data is to be read.
	 * @param ingestLogFile
	 *            File Object of the file on which the data is to be write.
	 * @throws IOException
	 * @throws IOException
	 */
	private void writeLogDataToIngestLogFile(StringBuffer logFileStrBuffer,
			File ingestLogFile) throws IOException {

		FileOutputStream fout = new FileOutputStream(ingestLogFile);

		byte[] byteArr = new byte[logFileStrBuffer.toString().length()];

		for (int i = 0; i < logFileStrBuffer.length(); i++) {
			byteArr[i] = (byte) logFileStrBuffer.charAt(i);

		}
		fout.write(byteArr);
		fout.close();
	}

	/**
	 * Method to replace the Attribute Names with Attribute values in Message
	 * 
	 * @param msg
	 * @return
	 */
	public String formatMail(String msg, IDfSysObject formObj,
			IDfSession mailSession, String formReqNo) {
		String loMsg = msg;
		try {
			
			while (loMsg.indexOf("{") != -1) {
				String formAttr = loMsg.substring(loMsg.indexOf("{") + 1, loMsg
						.indexOf("}"));
				String formAttrValue = "";
				if (formAttr.equals("wf_performer")) {
					formAttrValue = getLastWFPerformerName(mailSession,
							formAttr, formReqNo);
				} else {
					formAttrValue = formObj.getString(formAttr);
				}
				loMsg = loMsg.replace("{" + formAttr + "}", formAttrValue);
			}
		} catch (StringIndexOutOfBoundsException sioobe) {
			LOGGER.error("***Error***Please Check Properties File.");
			LOGGER.error(sioobe.getLocalizedMessage(), sioobe);
		} catch (Exception ex) {
			LOGGER.error(ex.getLocalizedMessage(), ex);
		}
		return loMsg;
	}

	/**
	 * Method to get the SMTP Server Value.
	 * 
	 * @param session
	 * @param hostQuery
	 * @return
	 */
	private String getSMTPServer(IDfSession session, String hostQuery) {
		String smtpServer = "";
		IDfCollection smtpServerCollection = null;

		try {
			IDfClientX cx = new DfClientX();

			IDfQuery dfQuery = cx.getQuery();
			dfQuery.setDQL(hostQuery);

			smtpServerCollection = dfQuery
					.execute(session, IDfQuery.READ_QUERY);

			if (smtpServerCollection != null) {
				if (smtpServerCollection.next()) {
					smtpServer = smtpServerCollection.getString("smtp_server");
				}
			}
		} catch (Exception ex) {
			LOGGER.error(ex.getMessage());
		} finally {
			try {
				if (smtpServerCollection != null
						&& (smtpServerCollection.getState() != IDfCollection.DF_CLOSED_STATE)) {
					smtpServerCollection.close();
				}
			} catch (DfException e) {
				LOGGER.error(e.getMessage());
			}
		}

		return smtpServer;
	}

}
