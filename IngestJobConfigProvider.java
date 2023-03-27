/**
 * 
 */
package com.pearson.ps.ingest.provider;

import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSessionManager;
import com.pearson.ps.ingest.exception.IngestException;

/**
 * @author Manav Leslie
 *
 */
public interface IngestJobConfigProvider {

	
	/** Returns the admin user name 
	 * @return String
	 */
	String getAdminUserName();

	
	/**Returns docbase name
	 * @return String
	 */
	String getDocbaseName();
	
	/**
	 * returns the admin session mananger  
	 * @return
	 */
	IDfSessionManager getAdminSessionManager();
	
	/**
	 * Returns the admin session  
	 * @return admin session 
	 */
	IDfSession getAdminSession();
	
	/**
	 * Returns the session for archivist user 
	 * 
	 * @return Session manager for archivist 
	 */
	IDfSessionManager getArchivistSessionManager(String archivistUserName) throws IngestException;
	
	/**
	 * 
	 * @return inbound URL used for BPS integration 
	 */
	String getInBoundURL();

	/**
	 * 
	 * @return number of threads per ingest job 
	 */
	int getIngestJobThreadCount();	
	
	/**
	 * 
	 * @return Active MQueue URL for connection.
	 */
	String getActiveMQueueURL();
	
	/**
	 * 
	 * @return Ingest Active MQueue Name
	 */
	String getIngestActiveMQueueName();
	
}
