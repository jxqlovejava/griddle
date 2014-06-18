package com.ximalaya.griddle.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件通用操作工具类
 * @author will
 *
 */
public class FileUtil {
	
	private static final String DUMP_FILE_FORMAT_SUFFIX = ".dat";
	
	private static final Logger LOG = LoggerFactory.getLogger(FileUtil.class);
	
	/**
	 * 判断指定路径的文件是否存在
	 * @param filePath
	 * @return
	 */
	public static boolean isFileExist(String filePath) {
		return new File(filePath).exists();
	}
	
	/**
	 * 获取文件完整路径
	 * @param fileDir
	 * @param fileName
	 * @param fileFormatSuffix 文件格式后缀，比如 .dat
	 * @return
	 */
	public static String getFullFilePath(String fileDir, String fileName, String fileFormatSuffix) {
		StringBuilder filePathBuilder = new StringBuilder();
		filePathBuilder.append(fileDir);
		filePathBuilder.append("/");
		filePathBuilder.append(fileName);
		filePathBuilder.append(fileFormatSuffix);
		
		return filePathBuilder.toString();
	}
	
	/**
	 * 删除指定路径的文件
	 * @param file
	 */
	public static void deleteFile(File file) {
		if(file != null && file.exists()) {
			LOG.debug("delete file: {}", file.getAbsolutePath());
			file.delete();
		}
	}
	
	/**
	 * 重命名文件，fromFile => toFile
	 * @param fromFile
	 * @param toFile
	 * @return
	 */
	public static boolean renameFile(File fromFile, File toFile) {
		LOG.debug("rename temp file: {} to target file: {}", 
				  fromFile.getAbsolutePath(), 
				  toFile.getAbsolutePath());
		
		if(fromFile != null && fromFile.exists()
		   && toFile != null) {
			return fromFile.renameTo(toFile);
		}
		
		return false;
	}
	
	/**
	 * 列出某个目录下的所有文件名列表
	 * @param dirPath
	 * @return
	 */
	public static List<String> listFiles(String dirPath) {
		File dirFile = new File(dirPath);
		File[] files = dirFile.listFiles();
		List<String> fileNameList = new ArrayList<String>();
		if(files != null && files.length > 0) {
			for(int i = 0; i < files.length; i++) {
				File curFile = files[i];
				if(curFile.isFile()) {   // 只处理文件
					fileNameList.add(files[i].getName());
				}
			}
		}
		
		return fileNameList;
	}
	
	/**
	 * 获取Dump文件格式后缀
	 * @return
	 */
	public static String getDumpFileFormatSuffix() {
		return DUMP_FILE_FORMAT_SUFFIX;
	}

}
