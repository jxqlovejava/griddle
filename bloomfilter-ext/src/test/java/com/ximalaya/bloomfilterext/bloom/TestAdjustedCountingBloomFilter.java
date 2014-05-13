package com.ximalaya.bloomfilterext.bloom;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;

import junit.framework.Assert;

import org.junit.Test;

import com.ximalaya.bloomfilterext.hash.Hash;

/**
 * 一个AdjustedCountingBloomFilter对象占用字节数估算：
 * VERSION:    1 int = 4 byte
 * nbHash:     1 int = 4 byte
 * hashType:   1 byte
 * vectorSize: 1 int = 4 byte
 * buckets:    vectorSize/4 byte
 * -------------------------------
 * total:      约为(vectorSize/2 + 13) byte
 * 所以：
 * 如果vectorSize为1<<24（1677.7216万），内存消耗大概为8MB
 * 如果vectorSize为1<<27（1.34亿），内存消耗大概为64MB
 * 
 * @author will
 */
public class TestAdjustedCountingBloomFilter {
	
	private static final int DEFAULT_VECTOR_SIZE = 1 << 10;
	private static final int FILE_BYTE_SIZE = (((DEFAULT_VECTOR_SIZE - 1) >>> 4) + 1) * 8 + 13;
	private static final int DEFAULT_HASH_NUM = 20;
	private static final int DEFAULT_HASH_TYPE = Hash.MURMUR_HASH;
	
	private static final String DUMP_FILE_PATH = "/usr/local/dump/dump.dat";
	
	@Test
	public void commonTest() throws IOException {
		AdjustedCountingBloomFilter acbf  = 
				new AdjustedCountingBloomFilter(DEFAULT_VECTOR_SIZE, DEFAULT_HASH_NUM, DEFAULT_HASH_TYPE);
		acbf.add(new Key("jxq".getBytes()));
		acbf.add(new Key("jxq".getBytes()));
		acbf.add(new Key("jxq".getBytes()));
		
		Assert.assertTrue(acbf.approximateCount(new Key("jxq".getBytes())) == 3);
	}
	
	@Test
	public void persistCBFToDisk() throws IOException {
		AdjustedCountingBloomFilter acbf  = 
				new AdjustedCountingBloomFilter(DEFAULT_VECTOR_SIZE, DEFAULT_HASH_NUM, DEFAULT_HASH_TYPE);
		acbf.add(new Key("hello".getBytes()));
		acbf.add(new Key("world".getBytes()));
		acbf.add(new Key("jxq".getBytes()));
		acbf.add(new Key("jxq".getBytes()));
		acbf.add(new Key("jxq".getBytes()));
		
		File dumpFile = new File(DUMP_FILE_PATH);
		if(dumpFile.exists()) {
			dumpFile.delete();
		}
		
		RandomAccessFile raf = new RandomAccessFile(dumpFile, "rw");
		FileChannel fileChannel = raf.getChannel();
		MappedByteBuffer mbb = fileChannel.map(MapMode.READ_WRITE, 0, FILE_BYTE_SIZE);
		
		acbf.write(mbb);
		unmap(mbb);
		raf.close();
	}
	
	@Test
	public void readCBFFromDisk() throws IOException {
		File dumpFile = new File(DUMP_FILE_PATH);
		AdjustedCountingBloomFilter acbf  = 
				new AdjustedCountingBloomFilter(DEFAULT_VECTOR_SIZE, DEFAULT_HASH_NUM, DEFAULT_HASH_TYPE);
		if(dumpFile.exists()) {
			RandomAccessFile raf = new RandomAccessFile(dumpFile, "rw");
			FileChannel fileChannel = raf.getChannel();
			MappedByteBuffer mbb = fileChannel.map(MapMode.READ_ONLY, 0, FILE_BYTE_SIZE);
			
			acbf.readFields(mbb);
			unmap(mbb);
			raf.close();
			
			// 测试
			Assert.assertTrue(acbf.membershipTest(new Key("hello".getBytes())));
			Assert.assertTrue(acbf.membershipTest(new Key("world".getBytes())));
			Assert.assertTrue(acbf.membershipTest(new Key("jxq".getBytes())));
			Assert.assertFalse(acbf.membershipTest(new Key("will".getBytes())));
			Assert.assertEquals(3, acbf.approximateCount(new Key("jxq".getBytes())));
		}
	}
	
	
	/**
	 * 在MappedByteBuffer释放后再对它进行读操作的话就会引发jvm crash，在并发情况下很容易发生
	 * 正在释放时另一个线程正开始读取，于是crash就发生了。所以为了系统稳定性释放前一般需要检
	 * 查是否还有线程在读或写
	 * @param mappedByteBuffer
	 */
	public static void unmap(final MappedByteBuffer mappedByteBuffer) {
		try {
			if (mappedByteBuffer == null) {
				return;
			}
			
			mappedByteBuffer.force();
			AccessController.doPrivileged(new PrivilegedAction<Object>() {
				@Override
				@SuppressWarnings("restriction")
				public Object run() {
					try {
						Method getCleanerMethod = mappedByteBuffer.getClass()
								.getMethod("cleaner", new Class[0]);
						getCleanerMethod.setAccessible(true);
						sun.misc.Cleaner cleaner = 
								(sun.misc.Cleaner) getCleanerMethod
									.invoke(mappedByteBuffer, new Object[0]);
						cleaner.clean();
						
					} catch (Exception e) {
						e.printStackTrace();
					}
					System.out.println("clean MappedByteBuffer completed");
					return null;
				}
			});

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
