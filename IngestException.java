/**
 * 
 */
package com.pearson.ps.ingest.exception;

import java.io.File;

/**
 * Custom exception class in which file can be passed. 
 * This will be used by the ingest threads to indicate that it failed to ingest 
 * particular file 
 * It also extends the other exception classes. 
 * @author Manav Leslie
 *
 */
@SuppressWarnings("serial")
public class IngestException extends PearsonException {
	
	private File file = null;

	/*
	 * returns the file associated with the exception  
	 */
	public File getFile() {
		return file;
	}


	/**
	 * @param message
	 * @param cause
	 */
	public IngestException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * @param message
	 */
	public IngestException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public IngestException(Throwable cause) {
		super(cause);
	}

	/**
	 * 
	 */
	public IngestException() {
	}

	/**
	 * @param File file associated with the exception 
	 */
	public IngestException(File file) {
		super(file.getAbsolutePath());
		this.file = file; 
		
	}

	
	/**
	 * @param message
	 * @param File file associated with the exception 
	 */
	public IngestException(String message, File file) {
		super(message);
		this.file = file; 
		
	}

}
