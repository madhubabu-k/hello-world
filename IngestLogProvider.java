/**
 * 
 */
package com.pearson.ps.ingest.provider;


/**
 * @author Manav Leslie
 *
 */
public interface IngestLogProvider {

	
 void appendMainMessage(String message);
 
 StringBuffer returnConsolidatedMessage();
 
 void appendFailedFilesMessage(String message);
 
 void appendFailedFilesMessageToMain();
 
 void appendFooterMessage(String message);
 
 void appendProductTempMessage(String message);
 
 void appendSuccessProductMainMessage();
 
 void appendFailedProductMainMessage();
 
 void appendFailedFilesMessageToTempBuffer();
 
 StringBuffer returnConsolidateBulkProductLog();
 
 void clearBuffers();
 	
}
