package com.ximalaya.bloomfilterext.bloom;

/**
 * 估算占用内存大小
 * @author jiangxiaoqiang
 *
 */
public class TestGetBucketSize {

	public static int buckets2words(int vectorSize) {
		return ((vectorSize - 1) >>> 4) + 1;
	}
	
	public static int getBucketSizeInByte(int vectorSize) {
		return buckets2words(vectorSize) * 8;
	}
	
	public static int getTotalSizeInByte(int vectorSize) {
		return getBucketSizeInByte(vectorSize) + 13;
	}
	
	public static void main(String[] args) {
		System.out.println(getTotalSizeInByte(1<<27) / (1024 * 1024) + "MB");
	}

}