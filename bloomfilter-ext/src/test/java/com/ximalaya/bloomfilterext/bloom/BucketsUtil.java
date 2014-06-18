package com.ximalaya.bloomfilterext.bloom;

public class BucketsUtil {
	
	public static int vectorSizeToBucketNum(int vectorSize) {
		return ((vectorSize - 1) >>> 4) + 1;
	}
	
	public static long bucketNumToVectorSize(long bucketNum) {
		return (bucketNum - 1) << 4;
	}
	
	public static long getFileSizeInByte(long bucketNum) {
		return bucketNumToVectorSize(bucketNum) * 8 + 13;
	}
	
	public static void main(String[] args) {
//		System.out.println(bucketNumToVectorSize(1000000L) / (1024 * 1024));
//		System.out.println(getFileSizeInByte(1000000L) / (1024 * 1024));
	}

}
