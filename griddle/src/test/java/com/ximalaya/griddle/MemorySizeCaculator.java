package com.ximalaya.griddle;

import java.util.concurrent.atomic.AtomicLongArray;

public class MemorySizeCaculator {
	
	public static void main(String[]args) throws InterruptedException {
		
		long startUsedMemoryInBytes = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		new AtomicLongArray(2000000);
		long endUsedMemoryInBytes = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		
		System.out.println((endUsedMemoryInBytes - startUsedMemoryInBytes) / 1024 / 1024 + "MB");
	}

}
