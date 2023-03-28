/**
 * 
 */
package com.pearson.ps.ingest.exception;

/**
 * Abstract custom exception class which will be the inherited by all 
 * custom exception classes. 
 * 
 * @author Manav Leslie
 *
 */
@SuppressWarnings("serial")
public abstract class PearsonException extends Exception {

	/**
	 * 
	 */
	public PearsonException() {
	}

	/**
	 * @param message
	 * @param cause
	 */
	public PearsonException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * @param message
	 */
	public PearsonException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public PearsonException(Throwable cause) {
		super(cause);
	}



}
