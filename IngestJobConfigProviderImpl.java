/**
 * 
 */
package com.pearson.ps.ingest.provider;

import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.documentum.fc.client.DfClient;
import com.documentum.fc.client.IDfClient;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSessionManager;
import com.documentum.fc.client.IDfUser;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfLoginInfo;
import com.documentum.fc.common.IDfLoginInfo;
import com.pearson.ps.ingest.exception.IngestException;
import com.pearson.ps.ingest.main.IngestConstants;
import com.pearson.ps.ingest.utils.TrustedAuthenticatorUtils;


/**
 * @author Manav Leslie
 * 
 * This class reads the properties file and provides the information to classes into 
 * which this provider is injected. 	
 *
 */
@SuppressWarnings("serial")
public class IngestJobConfigProviderImpl extends AbstractPropertiesProviderImpl  implements IngestJobConfigProvider {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(IngestJobConfigProviderImpl.class);

	private static final String ADMIN_USER_NAME = "ADMIN_USER_NAME";
	private static final String ADMIN_USER_PASSWORD = "ADMIN_USER_PWD";
	private static final String DOCBASE_NAME = "DOCBASE_NAME";
	private static final String INBOUND_URL = "INBOUND_URL";
	private static final String INGEST_JOB_THREADS = "INGEST_JOB_THREADS";
	private static final String ACTIVE_MQUEUE_URL = "ACTIVE_MQUEUE_URL";
	private static final String INGEST_MQUEUE_NAME = "INGEST_MQUEUE_NAME";

	
	private static IDfSessionManager adminSessionMgr;
	private static IDfSession adminSession = null;
	
	private static IDfSessionManager archivistSessionMgr = null;
	
	/**
	 * Constructor which will instantiate the session manager as well as 
	 * create an admin session which can then be used by all the other classes. 
	 * 
	 * @throws IngestException
	 */
	public IngestJobConfigProviderImpl() throws IngestException {
		
		super(IngestConstants.PS_INGEST_JOB_CONFIG_PROP_FILE);
		LOGGER.info("Initialising...");
        try {
			IDfClient dfClient = DfClient.getLocalClient();
			
			if (dfClient != null) {
				IDfLoginInfo li = new DfLoginInfo();
				li.setUser(getAdminUserName());
				li.setPassword(getAdminUserPwd());
				li.setDomain(null);

				adminSessionMgr = dfClient.newSessionManager();
				adminSessionMgr.setIdentity(getDocbaseName(), li);
				adminSession = adminSessionMgr.getSession(getDocbaseName());
			}			
		} catch (DfException dfe) {
			LOGGER.error("DF Exception creating a session",dfe);
			}		
	}

	public String getAdminUserName() {
		return getProperty(ADMIN_USER_NAME);
	}

	private String getAdminUserPwd() {
		return TrustedAuthenticatorUtils.decrypt(getProperty(ADMIN_USER_PASSWORD));
	}

	public String getDocbaseName() {
		return getProperty(DOCBASE_NAME);
	}

	public String getInBoundURL() {
		return getProperty(INBOUND_URL);
	}	
	
	@Override
	public IDfSessionManager getAdminSessionManager() {
		return adminSessionMgr;
	}

	@Override
	public IDfSession getAdminSession() {
		return adminSession;
	}

	/**
	 * Method to fetch the 
	 * @throws IngestException 
	 */
	@Override
	public IDfSessionManager getArchivistSessionManager(String archivistUserName) throws IngestException {
		//archivistSessionMgr = null;
		if (archivistSessionMgr == null) {
			String archivistLoginName = null;
			try {
				IDfClient dfClient = DfClient.getLocalClient();
				archivistSessionMgr = dfClient.newSessionManager();
				IDfLoginInfo loginInfo = new DfLoginInfo();

				IDfUser archivistUser = (IDfUser) getAdminSession()
						.getObjectByQualification(" prsn_user where user_name='"
								+ StringEscapeUtils.escapeSql(archivistUserName) + "'");
				if (archivistUser == null ) throw new IngestException("Error getting the Ingest Archivist User ");
				archivistLoginName = archivistUser.getString("user_login_name");

				loginInfo.setUser(archivistLoginName);
				String archivistLoginTicket = getAdminSession().getLoginTicketForUser(archivistLoginName);

				loginInfo.setPassword(archivistLoginTicket);
				loginInfo.setDomain(null);

				archivistSessionMgr.setIdentity(getDocbaseName(), loginInfo);
				archivistSessionMgr.authenticate(getDocbaseName());

			} catch (DfException ex) {
				LOGGER.error("Error getting Archivist SessionManager - Exception : ", ex);
				}			
		}


		return archivistSessionMgr;
	}

	@Override
	public int getIngestJobThreadCount() {
		int threadCount  = 5;
		String threadCountStr = getProperty(INGEST_JOB_THREADS);
		try {
			if (threadCountStr != null ) {
				threadCount = Integer.parseInt(threadCountStr);
			}
		} catch (NumberFormatException ne) {
			LOGGER.error("Error converting thread value to int .. Using default value of 5 ", ne);
		}
		return threadCount;
	}

	@Override
	public String getActiveMQueueURL() {
		return getProperty(ACTIVE_MQUEUE_URL);
	}

	@Override
	public String getIngestActiveMQueueName() {
		return getProperty(INGEST_MQUEUE_NAME);
	}

}
