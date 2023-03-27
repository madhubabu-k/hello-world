/**
 * 
 */
package com.pearson.ps.ingest.provider;

import com.pearson.ps.ingest.exception.IngestException;
import com.pearson.ps.ingest.main.IngestConstants;

/**
 * @author Manav Leslie
 *
 */
@SuppressWarnings("serial")
public class MessagesPropertiesProviderImpl extends GenericPropertiesProviderImpl implements GenericPropertiesProvider {

	public MessagesPropertiesProviderImpl() throws IngestException {
		super(IngestConstants.PRSN_INGEST_MESSAGE_PROP_FILE);
	}

}
