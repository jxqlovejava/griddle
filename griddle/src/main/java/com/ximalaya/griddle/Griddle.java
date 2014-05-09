package com.ximalaya.griddle;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ximalaya.bloomfilterext.bloom.AdjustedCountingBloomFilter;
import com.ximalaya.bloomfilterext.bloom.Key;
import com.ximalaya.griddle.exception.DumpFileFailedException;
import com.ximalaya.griddle.exception.RecycleGriddleFailedException;
import com.ximalaya.griddle.nio.MemoryMappedFile;
import com.ximalaya.griddle.util.FileUtil;

/**
 * 过滤器类，基于Counting Bloom Filter算法实现
 * @author will
 *
 */
public class Griddle {
	
	private int maxRepeatInsertCount;   // 最多可以重复插入同一个条目多少次
	private int vectorSize;             // 预计总条目数
	private int hashNum;
	private int hashType;
	
	private String dumpFileDir;
	private String dumpFileName;
	private int fileSizeInByte;
	
	private CBFSection cbfSection;   // CBF数据区，包装了一个Counting Bloom Filter实现类对象

	private AtomicBoolean hasRecycled = new AtomicBoolean(false);   // 是否已被回收
	private static final int RECYCLE_RETRY_TIMES = 3;              // 回收重试次数
	
	private static final Logger LOG = LoggerFactory.getLogger(Griddle.class);
	
	
	/*
	 * ------------------------------------------------------
	 * 构造函数
	 * ------------------------------------------------------
	 */
	
	private Griddle(int maxRepeatInsertCount, int vectorSize, int hashNum, int hashType, 
			String dumpFileDir, String dumpFileName) {
		if(maxRepeatInsertCount <= 0
		   || vectorSize <= 0
		   || hashNum <= 0
		   || hashType < 0
		   || StringUtils.isEmpty(dumpFileDir)
		   || StringUtils.isEmpty(dumpFileName)) {
			throw new IllegalArgumentException("all int type constructor params for " 
					+ "CBFBasedFilter should > 0 (hashType >= 0), dumpFileDir and dumpFileName should not empty");
		}
		
		this.maxRepeatInsertCount = maxRepeatInsertCount;
		this.vectorSize = vectorSize;
		this.hashNum = hashNum;
		this.hashType = hashType;
		
		this.dumpFileDir = dumpFileDir;
		this.dumpFileName = dumpFileName;
		this.fileSizeInByte = getCaculatedFileSizeInByte(this.vectorSize);
		
		this.cbfSection = createNewCBFSection();
	}
	
	private Griddle(int maxRepeatInsertCount, int vectorSize, int hashNum, int hashType, 
			String dumpFileDir, String dumpFileName, AdjustedCountingBloomFilter cbf) {
		if(maxRepeatInsertCount <= 0
		   || vectorSize <= 0
		   || hashNum <= 0
		   || hashType <= 0
		   || StringUtils.isEmpty(dumpFileDir)
		   || StringUtils.isEmpty(dumpFileName)
		   || cbf == null) {
			throw new IllegalArgumentException("all int type constructor params for " 
					+ "CBFBasedFilter should > 0, dumpFileDir and dumpFileName should not be empty, " 
					+ "cbf should not be null");
		}
		
		this.maxRepeatInsertCount = maxRepeatInsertCount;
		this.vectorSize = vectorSize;
		this.hashNum = hashNum;
		this.hashType = hashType;
		
		this.dumpFileDir = dumpFileDir;
		this.dumpFileName = dumpFileName;
		this.fileSizeInByte = getCaculatedFileSizeInByte(this.vectorSize);
		
		this.cbfSection = createNewCBFSection(cbf);
	}
	
	
	/*
	 * ------------------------------------------------------
	 * 使用Counting Bloom Filter的相关方法 
	 * ------------------------------------------------------
	 */
	
	/**
	 * 插入Key到Counting Bloom Filter中，如果Key为空或者已达到最大重复插入次数返回false表示插入失败，
	 * 否则返回true表示插入成功
	 * @param keyWord   待添加的关键词
	 * @return true（插入Key成功） or false（插入Key失败）
	 */
	public boolean add(String keyWord) {
		if(StringUtils.isEmpty(keyWord)) {
			return false;
		}
		
		boolean result = insertKey(cbfSection, new Key(keyWord.getBytes()));
		return result;
	}
	
	/**
	 * 获取某个Key剩余的可插入次数
	 * @param keyWord
	 * @return
	 */
	public int getRemainInsertCount(String keyWord) {
		if(StringUtils.isEmpty(keyWord)) {
			return -1;
		}
		
		int result = getHasInsertedCount(cbfSection, new Key(keyWord.getBytes()));
		return result;
	}
	
	/**
	 * 标记Griddle为可以回收，外部调用这个接口
	 */
	public void markToRecycle() {
		cbfSection.markToEnableCanGC();
	}
	
	/**
	 * 真正的回收方法：释放CBF占用内存，并删除对应的磁盘文件
	 */
	public void recycle() {
		if(hasRecycled()) {   // 不要重复进行回收
			throw new RecycleGriddleFailedException("duplicate recycle griddle: " + getDumpFileName());
		}
		
		/*
		 * 必须同时满足canGC为true且useCount为0才可以回收
		 */
		if(cbfSection.canGC() && cbfSection.getUseCount() == 0) {
			LOG.debug("recycle griddle: {}", getDumpFileName());
			cbfSection = null;   // 释放cbfSection占用的内存
			
			int retriedTimes = 0;
			Throwable throwable = null;
			while(retriedTimes < RECYCLE_RETRY_TIMES) {   // 删除文件失败则重试
				String toDeleteDumpFilePath = getFullDumpFilePath(this.dumpFileDir, this.dumpFileName);
				File dumpFile = new File(toDeleteDumpFilePath);
				try {
					/*
					 * 删除对应的磁盘Dump文件
					 */
					FileUtil.deleteFile(dumpFile);
					break;
				}
				catch(Exception e) {
					retriedTimes++;
					LOG.error(String.format("delete dump file [%s] failed, has retried times: %d", 
											toDeleteDumpFilePath,
											retriedTimes), 
							  e);
					throwable = e;
				}
			}
			
			if(retriedTimes == RECYCLE_RETRY_TIMES) {   // 重试后仍失败则抛出异常
				throw new RecycleGriddleFailedException(String.format("recycle failed after exhaust retry times, griddleName: %s", 
																		getDumpFileName()), 
														 throwable);
			}
			
			setHasRecycled();   // 标记当前Griddle为已被回收
		}
	}
	
	/**
	 * 创建CBFSection对象
	 * @return
	 */
	private CBFSection createNewCBFSection() {
		return new CBFSection(new AdjustedCountingBloomFilter(this.vectorSize, this.hashNum, this.hashType));
	}
	
	/**
	 * 创建CBFSection对象重载方法
	 * @param cbf
	 * @return
	 */
	private CBFSection createNewCBFSection(AdjustedCountingBloomFilter cbf) {
		return new CBFSection(cbf);
	}
	
	/**
	 * 往CBFSection中插入Key，如果未达到最大重复插入次数，则允许插入并返回true；返回返回false。
	 * 操作前后分别进行计数增1和减1
	 * @param section
	 * @param key
	 * @return
	 */
	private boolean insertKey(CBFSection section, Key key) {
		section.increaseUseCount();
		
		if(section.getInsertedCount(key) >= getMaxRepeatInsertCount()) {
			section.decreaseUseCount();
			return false;
		}
		
		section.insertKey(key);
		section.decreaseUseCount();
		return true;
	}
	
	/**
	 * 获取某个Key已被重复插入次数
	 * @param section
	 * @param key
	 * @return 该Key已被重复插入次数
	 */
	private int getHasInsertedCount(CBFSection section, Key key) {
		int result = 0;
		section.increaseUseCount();
		result = section.getInsertedCount(key);
		section.decreaseUseCount();
		return result;
	}
	
	
	/*
	 * ------------------------------------------------------
	 * Dump CBF相关方法
	 * ------------------------------------------------------
	 */
	
	/**
	 * Dump CBF到磁盘文件，始终只Dump包含最新数据的CBF
	 */
	public void dumpCBFToDisk() {
		String dumpFilePath = getFullDumpFilePath(dumpFileDir, dumpFileName);
		String tmpDumpFilePath = dumpFilePath + ".tmp";
		
		LOG.debug("dump cbf to file [{}]", dumpFilePath);
		
		MemoryMappedFile memoryMappedFile = new MemoryMappedFile(tmpDumpFilePath, this.fileSizeInByte);
		memoryMappedFile.map();
		AdjustedCountingBloomFilter cbf = this.cbfSection.getCBF();
		try {
			cbf.write(memoryMappedFile.getMappedByteBuffer());
		} catch (IOException e) {
			String errorMsg = "dump CBF to file [" + dumpFilePath + "] failed: " + e.getMessage();
			LOG.error(errorMsg, e);
			throw new DumpFileFailedException(errorMsg, e);
		} finally {
			memoryMappedFile.unmap();
		}
		
		File oldDumpFile = new File(dumpFilePath);
		File tmpDumpFile = new File(tmpDumpFilePath);
		FileUtil.deleteFile(oldDumpFile);
		boolean renameResult = FileUtil.renameFile(tmpDumpFile, oldDumpFile);
		if(!renameResult) {   // 重命名失败
			String errorMsg = "dump CBF to file [" + dumpFilePath + "] failed: rename failed";
			LOG.error(errorMsg);
			throw new DumpFileFailedException(errorMsg);
		}
	}
	
	/**
	 * 从磁盘文件恢复Griddle对象或在发生异常时从头开始构建Griddle对象，异常情况包括下面几种：
	 * <li>对应的Dump文件不存在</li>
	 * <li>传入的全局CBF配置属性与从Dump文件中恢复的CBF的属性值不一致</li>
	 * <li>从Dump文件恢复发生异常</li>
	 * @param maxRepeatInsertCount
	 * @param vectorSize
	 * @param hashNum
	 * @param hashType
	 * @param dumpFileDir
	 * @param dumpFileName
	 * @return
	 */
	public static Griddle restoreFromDumpFileOrConstructFromGroundIfException(int maxRepeatInsertCount, 
			int vectorSize, int hashNum, int hashType, String dumpFileDir, String dumpFileName) {
		if(maxRepeatInsertCount <= 0
		   || vectorSize <= 0
		   || hashNum <= 0
		   || hashType < 0
		   || StringUtils.isEmpty(dumpFileDir)
		   || StringUtils.isEmpty(dumpFileName)) {
			throw new IllegalArgumentException("all int type parameters should > 0 (hashType >= 0), " 
					+ "dumpFileDir & dumpFileName should not empty");
		}
		
		Griddle griddle = null;
		String dumpFilePath = getFullDumpFilePath(dumpFileDir, dumpFileName);
		if(FileUtil.isFileExist(dumpFilePath)) {   // 存在对应的Dump文件，则尝试从Dump文件恢复Griddle对象
			LOG.info("try to restore Griddle from dump file: {}", dumpFilePath);
			
			int fileSizeInByte = getCaculatedFileSizeInByte(vectorSize);
			MemoryMappedFile memoryMappedFile = new MemoryMappedFile(dumpFilePath, fileSizeInByte);
			memoryMappedFile.map();
			AdjustedCountingBloomFilter cbf = new AdjustedCountingBloomFilter();
			try {
				cbf.readFields(memoryMappedFile.getMappedByteBuffer());
				if(cbf.getVectorSize() != vectorSize 
				   || cbf.getNbHash() != hashNum 
				   || cbf.getHashType() != hashType) {   // 如果从Dump文件恢复出的CBF配置和现在传入的配置不一致，则由零构建
					griddle = constructFromGround(maxRepeatInsertCount, vectorSize, hashNum, hashType, dumpFileDir, 
							dumpFileName);
				}
				else {
					griddle = new Griddle(maxRepeatInsertCount, vectorSize, hashNum, hashType, dumpFileDir, 
							dumpFileName, cbf);
				}
			} catch (Exception e) {
				LOG.error("restore Griddle from file [" + dumpFilePath + "] failed, to construct from ground on", e);
				
				// 恢复发生异常，则也从零开始新建
				griddle = constructFromGround(maxRepeatInsertCount, vectorSize, hashNum, hashType, dumpFileDir, 
						dumpFileName);
			} finally {
				memoryMappedFile.unmap();
			}
		}
		else {   // 不存在Dump文件则从零开始构建
			LOG.info("doesn't exist dump file: {}, construct CBFBaseFilter from ground on: ",
					 dumpFileName);
			
			griddle = constructFromGround(maxRepeatInsertCount, vectorSize, hashNum, hashType, dumpFileDir, 
					dumpFileName);
		}
		
		return griddle;
	}
	
	public static Griddle constructFromGround(int maxRepeatInsertCount, int vectorSize, int hashNum, 
			int hashType, String dumpFileDir, String dumpFileName) {
		if(maxRepeatInsertCount <= 0
		   || vectorSize <= 0
		   || hashNum <= 0
		   || hashType < 0
		   || StringUtils.isEmpty(dumpFileDir)
		   || StringUtils.isEmpty(dumpFileName)) {
			throw new IllegalArgumentException("all int type parameters should > 0 (hashType >= 0), " 
					+ "dumpFileDir & dumpFileName should not empty");
		}
		
		return new Griddle(maxRepeatInsertCount, vectorSize, hashNum, hashType, dumpFileDir, dumpFileName);
	}
	
	private final static String getFullDumpFilePath(String dumpFileDir, String dumpFileName) {
		StringBuilder filePathBuilder = new StringBuilder();
		filePathBuilder.append(dumpFileDir);
		if(!dumpFileDir.endsWith(File.separator)) {
			filePathBuilder.append(File.separator);
		}
		filePathBuilder.append(dumpFileName);
		
		return filePathBuilder.toString();
	}
	
	/**
	 * 获取预期的文件大小（单位：字节）
	 * 计算方法参考AdjustedCountingBloomFilter的序列化反序列化方法
	 * @return
	 */
	private final static int getCaculatedFileSizeInByte(int vectorSize) {
		return ( ( (vectorSize - 1) >>> 4 ) + 1 ) * 8 + 13;
	}
	
	
	/*
	 * ------------------------------------------------------
	 * Getters/Setters
	 * ------------------------------------------------------
	 */

	public int getMaxRepeatInsertCount() {
		return maxRepeatInsertCount;
	}

	public void setMaxRepeatInsertCount(int maxRepeatInsertCount) {
		this.maxRepeatInsertCount = maxRepeatInsertCount;
	}

	public int getVectorSize() {
		return vectorSize;
	}

	public void setVectorSize(int vectorSize) {
		this.vectorSize = vectorSize;
	}

	public int getHashNum() {
		return hashNum;
	}

	public void setHashNum(int hashNum) {
		this.hashNum = hashNum;
	}

	public int getHashType() {
		return hashType;
	}

	public void setHashType(int hashType) {
		this.hashType = hashType;
	}
	
	public String getDumpFileName() {
		return dumpFileName;
	}
	
	public int getFileSizeInByte() {
		return fileSizeInByte;
	}
	
	public CBFSection getCBFSection() {
		return cbfSection;
	}
	
	public boolean hasRecycled() {
		return hasRecycled.get();
	}
	
	public void setHasRecycled() {
		hasRecycled.set(true);
	}
	
}