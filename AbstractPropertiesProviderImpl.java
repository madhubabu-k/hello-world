/**
 * 
 */
package com.pearson.ps.ingest.provider;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashSet;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pearson.ps.ingest.exception.IngestException;

/**
 * Abstract super class used by all the classes which needs to load properties file.
 * @author Manav Leslie
 * 
 *
 */
@SuppressWarnings("serial")
public abstract class AbstractPropertiesProviderImpl extends Properties {
	
	protected LinkedHashSet<Object> orderedKeysSet = new LinkedHashSet<Object>();
	
	private String propertiesFileName = "";
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPropertiesProviderImpl.class);

	/**
	 * Constructor 
	 * @param propertiesFileName
	 * @throws IngestException 
	 */
	public AbstractPropertiesProviderImpl(String propertiesFileName) throws IngestException {
		super();
		//this.propertiesFileName = propertiesFileName;
		setPropertiesFileName(propertiesFileName);
		
		ClassLoader loader = this.getClass().getClassLoader();
        InputStream propStream = loader.getResourceAsStream(getPropertiesFileName());
        try {
			load(propStream);
		} catch (IOException e) {
			LOGGER.error("Failed to load properties file : {} ", propertiesFileName,e);
			throw new IngestException("Failed to load properties file", e);
		}		
		
	}

	/*
	 * Fetch the name of the properties file 
	 */
	public String getPropertiesFileName() {
		return propertiesFileName;
	}

	/**
	 * Sets the name of the properties file 
	 * @param propertiesFileName Properties file to be used. 
	 */
	public void setPropertiesFileName(String propertiesFileName) {
		this.propertiesFileName = propertiesFileName;
	} 

	/**
	 * Override Method to add the keys in ordered Linked Hash Set.
	 */
	@Override
	public synchronized Object put(Object key, Object value) {
		orderedKeysSet.add(key);
		return super.put(key, value);
	}
	
}
