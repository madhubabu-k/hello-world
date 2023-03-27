/**
 * 
 */
package com.pearson.ps.ingest.provider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.pearson.ps.ingest.exception.IngestException;

/**
 * @author Manav Leslie
 *
 */
@SuppressWarnings("serial")
public class GenericPropertiesProviderImpl extends AbstractPropertiesProviderImpl implements GenericPropertiesProvider {

	public GenericPropertiesProviderImpl(String propertiesFileName) throws IngestException {
		super(propertiesFileName);
	}
	
	
	/* 
	 * @see com.pearson.ps.ingest.provider.GenericPropertiesProvider#getAllKeysAsArray()
	 */
	@Override
	public List<String> getAllKeysAsArray()   {

		List<String> mandatoryProdInfoAttrNamesList = new ArrayList<String>();
		for (Entry<Object, Object> entry : this.entrySet()) {
			String key = (String) entry.getKey();
			mandatoryProdInfoAttrNamesList.add(key);
		}

		return mandatoryProdInfoAttrNamesList;
	}

	
	
	/* 
	 * @see com.pearson.ps.ingest.provider.GenericPropertiesProvider#getAllEntrySetAsArray()
	 */
	@Override
	public Map<String, String> getAllEntrySetAsArray()   {

		Map<String, String> metaDataPropMap = new HashMap<String, String>();
		for (Entry<Object, Object> entry : this.entrySet()) {
			String key = (String) entry.getKey();
			metaDataPropMap.put(key.trim(),((String) entry.getValue()).trim());
		}

		return metaDataPropMap;
	}

	/**
	 * @see com.pearson.ps.ingest.provider.GenericPropertiesProvider#getAllKeysAsOrderedList()
	 */
	@Override
	public List<String> getAllKeysAsOrderedList() {
		
		List<String> orderedKeysList = new ArrayList<String>();
		
		Object[] orderedKeys = orderedKeysSet.toArray();
		
		for(int count = 0; count < orderedKeys.length; count++)
		{
			orderedKeysList.add((String)orderedKeys[count]);
		}
		return orderedKeysList;
	}	
}
