package com.ximalaya.bloomfilterext.bloom;

import org.junit.Test;

import com.ximalaya.bloomfilterext.hash.Hash;

public class TestErrorCountRate {
	
	private static final int DEFAULT_HASH_TYPE = Hash.MURMUR_HASH;
	
	private static int[] vectorSizeCandidates = new int[] { 1 << 20, 1 << 25, 1 << 27 };
	private static int[] hashNumCandidates = new int[] { 8, 10, 12, 15, 20 };
	
	/**
	 * 测试计数错误概率
	 */
	@Test
	public void testErrorCountRate() {
		for(int i = 0; i < vectorSizeCandidates.length; i++) {
			int curVectorSize = vectorSizeCandidates[i];
			int curBucketSize = BucketsUtil.vectorSizeToBucketNum(curVectorSize);
			for(int j = 0; j < hashNumCandidates.length; j++) {
				int curHashNum = hashNumCandidates[j];
					
					CountingBloomFilter cbf = new CountingBloomFilter(curVectorSize, curHashNum, DEFAULT_HASH_TYPE);
					for(int m = 0; m < curBucketSize; m++) {
						for(int n = 0; n < 3; n++) {
							cbf.add(new Key(Integer.toString(m).getBytes()));
						}
					}
					
					int curErrorCountNum = 0;
					for(int m = 0; m < curBucketSize; m++) {
						if(cbf.approximateCount(new Key(Integer.toString(m).getBytes())) != 3) {
							curErrorCountNum++;
						}
					}
					
					System.out.println("bucketSize: " + curBucketSize + ", errorCountNum: " + curErrorCountNum
							+ ", hashNum: " + curHashNum 
							+ ", errorRate: " + (((double) curErrorCountNum/curBucketSize) * 100) + "%");
			}
			
			System.out.println();
		}
	}
	
}
