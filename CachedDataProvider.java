/**
 * 
 */
package com.pearson.ps.ingest.provider;

import com.documentum.fc.client.IDfSession;
import com.documentum.fc.common.DfException;
import com.pearson.ps.ingest.exception.IngestException;

/**
 * Populate   
 * 
 * @author Manav Leslie
 *
 */
public interface CachedDataProvider {
	
	void populateExtensionMappingMap(String bu_id, IDfSession tempSession)
		    throws DfException;
	
	String getObjTypeForExtension(String extension) throws IngestException;
	
	//Map<String, String> loadMetaFromProps(String strBuName) throws IOException, IngestException ;
	
	String getPropertyFileName(String buName, boolean isBulkIngest);

}
