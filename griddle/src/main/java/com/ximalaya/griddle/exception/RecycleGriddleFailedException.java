package com.ximalaya.griddle.exception;

public class RecycleGriddleFailedException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1969802542307175961L;
	
	public RecycleGriddleFailedException(String message) {
		super(message);
	}
	
	public RecycleGriddleFailedException(String message, Throwable cause) {
		super(message, cause);
	}

}
