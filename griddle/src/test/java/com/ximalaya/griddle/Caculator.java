package com.ximalaya.griddle;

public class Caculator {
	
	/**
	 * 根据Bucket Size计算Vector Size
	 * @param bucketSize
	 * @return
	 */
	public static int getVectorSize(int bucketSize) {
		return ((bucketSize - 1) << 4) + 1;
	}
	
	/**
	 * 根据Vector Size计算Bucket Size
	 * @param vectorSize
	 * @return
	 */
	public static int getBucketSize(int vectorSize) {
		return ((vectorSize - 1) >>> 4) + 1;
	}
	
	/**
	 * 根据Bucket Size计算Dump文件大小
	 * @param bucketSize
	 * @return
	 */
	public static int getFileSizeInByte(int bucketSize) {
		return ( ( ( getVectorSize(bucketSize) - 1 ) >>> 4 ) + 1 ) * 8 + 13;
	}
	
	/**
	 * 根据Vector Size计算Dump文件大小
	 * @param vectorSize
	 * @return
	 */
	public static int getFileSizeInByteByVectorSize(int vectorSize) {
		return ( ( ( vectorSize - 1 ) >>> 4 ) + 1 ) * 8 + 13;
	}
	
	public static void main(String[] args) {
		System.out.println(getFileSizeInByte(10000000) / (1024 * 1024) + "MB");
		System.out.println(76.2d * 15);   // 1140MB 
		
		System.out.println(getVectorSize(10000000));   // bucketSize: 10000000, vectorSize: 159999985
		System.out.println(getBucketSize(160000000));   // vectorSize: 160000000
		
		System.out.println(getFileSizeInByteByVectorSize(160000000) * 3L * 15 / 1024 / 1024 + "MB");
		System.out.println(getFileSizeInByteByVectorSize(1600000) * 3L * 15 / 1024 / 1024 + "MB");
		System.out.println();
		
		System.out.println(getBucketSize(160000000));
	}

}
