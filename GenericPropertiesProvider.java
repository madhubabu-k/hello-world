package com.pearson.ps.ingest.provider;

import java.io.IOException;
import java.util.List;
import java.util.Map;


public interface GenericPropertiesProvider extends Map<Object,Object> {

	/**To get Product Info
	 * @return arraylist
	 * @throws IngestException 
	 * @throws IOException
	 */
	List<String> getAllKeysAsArray();

	/**To get Product Info
	 * @return arraylist
	 * @throws IngestException 
	 * @throws IOException
	 */
	Map<String, String> getAllEntrySetAsArray();

	String getProperty(String string);

	/**
	 * Method to get the Keys as Ordered List.
	 * @return orderedKeysList
	 */
	List<String> getAllKeysAsOrderedList();
	
}