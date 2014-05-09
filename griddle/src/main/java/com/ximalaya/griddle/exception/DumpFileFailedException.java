package com.ximalaya.griddle.exception;

/**
 * Dump文件异常
 * @author will
 *
 */
public class DumpFileFailedException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7810970027271234078L;
	
	public DumpFileFailedException(String message) {
		super(message);
	}
	
	public DumpFileFailedException(String message, Throwable cause) {
		super(message, cause);
	}
	
}