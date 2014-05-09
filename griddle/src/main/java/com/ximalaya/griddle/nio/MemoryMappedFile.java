package com.ximalaya.griddle.nio;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 内存映射文件抽象类
 * @author will
 *
 */
public class MemoryMappedFile {
	
	private String filePath;      // 文件路径
	private File file;            // 文件对象
	private FileChannel fileChannel;
	private int fileSize;
	private MappedByteBuffer mappedByteBuffer;
	
	private static final Logger LOG = LoggerFactory.getLogger(MemoryMappedFile.class);
	
	public MemoryMappedFile(String filePath, int fileSize) {
		if(StringUtils.isEmpty(filePath)
		   || fileSize <= 0) {
			throw new IllegalArgumentException("filePath should not empty, fileSize should > 0");
		}
		
		this.filePath = filePath;
		this.fileSize = fileSize;
		this.file = new File(filePath);
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				LOG.error("create dump file [{}] failed: {}", filePath, e.getMessage());
			}
		}
	}
	
	/**
	 * 映射文件，使用时建议与unmap()成对出现
	 */
	public void map() {
		LOG.debug("map file: {}", filePath);
		
		try {
			RandomAccessFile raf = new RandomAccessFile(file, "rw");
			this.fileChannel = raf.getChannel();
		} catch (Exception e) {
			LOG.error("build file channel for file [{}] failed: {}", filePath, e.getMessage());
		}
		try {
			this.mappedByteBuffer = this.fileChannel
										 .map(FileChannel.MapMode.READ_WRITE, 0, this.fileSize);
		} catch (IOException e) {
			LOG.error("map file [{}]'s FileChannel to MappedByteBuffer failed: {}", filePath, e.getMessage());
		}
	}
	
	/**
	 * unmap文件
	 * @param mappedByteBuffer
	 */
	public void unmap() {
		LOG.debug("unmap file: {}", filePath);
		
		try {
			if (this.mappedByteBuffer == null) {
				return;
			}
			
			this.mappedByteBuffer.force();
			AccessController.doPrivileged(new PrivilegedAction<Object>() {
				@Override
				@SuppressWarnings("restriction")
				public Object run() {
					try {
						Method getCleanerMethod = mappedByteBuffer.getClass()
								.getMethod("cleaner", new Class[0]);
						getCleanerMethod.setAccessible(true);
						sun.misc.Cleaner cleaner = (sun.misc.Cleaner) getCleanerMethod
																		.invoke(mappedByteBuffer, new Object[0]);
						cleaner.clean();
						
					} catch (Exception e) {
						LOG.error("execute MappedByteBuffer's cleaner method for file [{}] failed: {}", 
								filePath, e.getMessage());
					}
					
					return null;
				}
			});
		} catch (Exception e) {
			LOG.error("exception occurs when unmap MappedByteBuffer for file [{}]： {}", filePath, e.getMessage());
		} finally {
			try {
				this.fileChannel.close();
			} catch (IOException e) {
				LOG.error("close file channel failed for file [{}]: {}", filePath, e.getMessage());
			}
		}
	}
	
	/*
	 * ------------------------------------------------------
	 * Getters
	 * ------------------------------------------------------
	 */
	public String getFilePath() {
		return this.filePath;
	}
	
	public File getFile() {
		return this.file;
	}
	
	public FileChannel getFileChannel() {
		return this.fileChannel;
	}
	
	public MappedByteBuffer getMappedByteBuffer() {
		return this.mappedByteBuffer;
	}
	
}
