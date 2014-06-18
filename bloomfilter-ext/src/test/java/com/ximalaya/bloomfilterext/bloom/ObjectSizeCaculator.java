package com.ximalaya.bloomfilterext.bloom;

import java.util.concurrent.atomic.AtomicLongArray;

public class ObjectSizeCaculator {
	
	private long startUsedSizeInByte;
	private long endUsedSizeInByte;
	private long totalUsedSizeInBytes;
	
	private static final ObjectSizeCaculator INSTANCE = new ObjectSizeCaculator();
	
	private ObjectSizeCaculator() {
	}
	
	public static ObjectSizeCaculator getInstance() {
		return INSTANCE;
	}
	
	public void start() {
		startUsedSizeInByte = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
	}
	
	public void end() {
		endUsedSizeInByte = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		totalUsedSizeInBytes = endUsedSizeInByte - startUsedSizeInByte;
	}
	
	public void printConsumedMemoryInKB() {
		System.out.println("Total memory consumed: " + (totalUsedSizeInBytes / 1024) + "KB");
	}
	
	public void printConsumedMemoryInMB() {
		System.out.println("Total memory consumed: " + (totalUsedSizeInBytes / 1024 / 1024) + "MB");
	}
	
	public static void main(String[] args) {
		ObjectSizeCaculator memorySizeCaculator = ObjectSizeCaculator.getInstance();
		memorySizeCaculator.start();
		
		new AtomicLongArray(2000000);
		
		memorySizeCaculator.end();
		memorySizeCaculator.printConsumedMemoryInKB();
		memorySizeCaculator.printConsumedMemoryInMB();
	}

}
