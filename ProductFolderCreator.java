package com.pearson.ps.ingest.creator;

import java.io.IOException;

import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.client.IDfTypedObject;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.IDfId;
import com.pearson.ps.ingest.exception.IngestException;

/**
 * Interface for the product folder creator class 
 * 
 * @author Manav Leslie
 *
 */
public interface ProductFolderCreator {

	/**
	 * Method to create the product folder 
	 * @param loFormReqNoObj Form Object 
	 * @param ingestSession Session as the ingest user
	 * @param lifecycleId IDfIf of the lifecycle to be attached
	 * @throws DfException 
	 * @throws IngestException
	 * @throws IOException
	 * @throws Exception
	 */
	IDfId createProductFolder(IDfSysObject loFormReqNoObj, IDfSession ingestSession, IDfId lifecycleId)
			throws DfException, IngestException, IOException;
	
	IDfId createBulkProductFolder(IDfSysObject loFormReqNoObj, String strBulkProductName, IDfSession ingestSession, IDfId lifecycleId, IDfTypedObject productMetadataCollection)throws DfException, IngestException, IOException;

}