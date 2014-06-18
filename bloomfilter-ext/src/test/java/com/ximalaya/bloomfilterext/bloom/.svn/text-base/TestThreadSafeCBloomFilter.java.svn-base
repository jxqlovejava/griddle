package com.ximalaya.bloomfilterext.bloom;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import com.ximalaya.bloomfilterext.hash.Hash;

public class TestThreadSafeCBloomFilter {
	
	private static final int VECTOR_SIZE = 1 << 20;
	private static final int DEFAULT_HASH_NUM = 20;
	private static final int DEFAULT_HASH_TYPE = Hash.MURMUR_HASH;
	
	@Test
	public void commonTest() throws IOException {
		ThreadSafeCBloomFilter tscb  = 
				new ThreadSafeCBloomFilter(VECTOR_SIZE, DEFAULT_HASH_NUM, DEFAULT_HASH_TYPE);
		tscb.add(new Key("jxq".getBytes()));
		tscb.add(new Key("jxq".getBytes()));
		tscb.add(new Key("jxq".getBytes()));
		
		Assert.assertTrue(tscb.approximateCount(new Key("jxq".getBytes())) == 3);
		Assert.assertTrue(tscb.approximateCount(new Key("will".getBytes())) == 0);
	}

}
