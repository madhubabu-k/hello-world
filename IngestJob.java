/**
 * 
 */
package com.pearson.ps.ingest.main;

import java.util.Date;

/**
 * The interface for the ingest job. 
 * 
 * @author Manav Leslie
 *
 */
public interface IngestJob {
	
	/**
	 * Method called to start the ingest process. 
	 */
	void startIngest();
	
	public void ingestSelectedProduct(String formObjectId, String correlationId, String workflowId, Date lastAccessDate, Date validDate);

}
