package com.ximalaya.griddle.exception;

public class MapOrUnmapFileFailedException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3842558441596393395L;
	
	public MapOrUnmapFileFailedException(String message) {
		super(message);
	}
	
	public MapOrUnmapFileFailedException(String message, Throwable cause) {
		super(message, cause);
	}

}
