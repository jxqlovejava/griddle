package com.ximalaya.griddle;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.scheduling.concurrent.ScheduledExecutorFactoryBean;
import org.springframework.scheduling.concurrent.ScheduledExecutorTask;

import com.ximalaya.griddle.util.FileUtil;

/**
 * 过滤器管理类
 * @author will
 *
 */
@Configuration
public class GriddleManager implements SmartLifecycle, ApplicationContextAware {
	
	/*
	 * 全局配置，在griddle-config.properties中配置
	 */
	private static String dumpFileDir;               // Dump文件目录
	private static long dumpFileIntervalMillis;      // 定时Dump时间间隔，单位为毫秒
	private static long recycleGriddleCheckMillis;   // 定时检查是否可回收Griddle的时间间隔 
	private static int vectorSize;                   // 预计每种过滤器插入最大次数
	private static int hashType;                     // 哈希函数类型，1-MurMur Hash，0-Jekins Hash
	private static int hashNum;                      // 重复进行哈希运算次数
	
	private static Map<String, Griddle> griddleMap = new ConcurrentHashMap<String, Griddle> ();   // Griddle名称到Griddle对象的映射
	
	private static AtomicBoolean hasStarted = new AtomicBoolean(false);
	private static AtomicBoolean isRunning = new AtomicBoolean(false);
	private static AtomicBoolean handoffRecycleGriddles = new AtomicBoolean(false);   // 等待回收Griddles
	
	private static final Object accessDumpFileMutex = new Object();   // 访问Dump文件的互斥锁
	
	private static final Logger LOG = LoggerFactory.getLogger(GriddleManager.class);
	
	
	/*
	 * ------------------------------------------------------
	 * 全局配置属性Setters
	 * ------------------------------------------------------
	 */
	@Autowired
	public void setDumpFileDir(
			@Value("${griddle.config.dumpFileDir}") String dumpFileDir) {
		GriddleManager.dumpFileDir = dumpFileDir;
		File dumpFileDirectory = new File(dumpFileDir);
		if(!dumpFileDirectory.exists() || !dumpFileDirectory.isDirectory()) {
			boolean createDirResult = dumpFileDirectory.mkdirs();
			if(!createDirResult) {
				throw new RuntimeException(String.format("create dump file dir failed: %s", dumpFileDir));
			}
		}
	}
	
	@Autowired
	public void setDumpFileIntervalMillis(
			@Value("${griddle.config.dumpFileIntervalMillis}") long dumpFileIntervalMillis) {
		GriddleManager.dumpFileIntervalMillis = dumpFileIntervalMillis;
	}
	
	@Autowired
	public void setRecycleGriddleCheckMillis(
			@Value("${griddle.config.recycleGriddleCheckMillis}") long recycleGriddleCheckMillis) {
		GriddleManager.recycleGriddleCheckMillis = recycleGriddleCheckMillis;
	}
	
	@Autowired
	public void setVectorSize(
			@Value("${griddle.config.vectorSize}") int vectorSize) {
		GriddleManager.vectorSize = vectorSize;
	}
	
	@Autowired
	public void setHashType(
			@Value("${griddle.config.hashType:1}") int hashType) {
		GriddleManager.hashType = hashType;
	}
	
	@Autowired
	public void setHashNum(
			@Value("${griddle.config.hashNum:20}") int hashNum) {
		GriddleManager.hashNum = hashNum;
	}
	
	
	/*
	 * ------------------------------------------------------
	 * SmartLifecycle和ApplicationContextAware接口方法实现
	 * ------------------------------------------------------
	 */
	
	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		((AbstractApplicationContext) applicationContext).registerShutdownHook();   // 注册ShutDownHook，以使Spring容器关闭时会调用stop()
		
		LOG.info("register shutdown hook success");
	}
	
	@Override
	public void start() {
		LOG.info("GriddleManager starting...");
		
		// 读取dumpFileDir目录下的所有Dump文件，并设置griddleMap
		List<String> dumpFileNameList = FileUtil.listFiles(dumpFileDir);
		if(!dumpFileNameList.isEmpty()) {
			for(String dumpFileName: dumpFileNameList) {
				String[] segments = dumpFileName.split("\\.");
				if(segments != null && segments.length == 3) {
					String curGriddleName = segments[0];
					int curMaxRepeatInsertCount = Integer.parseInt(segments[1]);
					
					Griddle griddle = null;
					synchronized (accessDumpFileMutex) {
						griddle = Griddle.restoreFromDumpFileOrConstructFromGroundIfException(curMaxRepeatInsertCount, 
																				   vectorSize,
																				   hashNum,
																				   hashType, 
																				   dumpFileDir,
																			 	   dumpFileName);
					}
					
					griddleMap.put(curGriddleName, griddle);
				}
			}
		}
		
		isRunning.set(true);
		hasStarted.set(true);
		
		LOG.info("GriddleManager has started");
	}

	@Override
	public void stop() {
		LOG.info("GriddleManager stop");
		
		if(isRunning.get()) {
			isRunning.set(false);
			
			// 关闭定时调度任务（不需要，会自动调用）
			
			// 如果当前还有回收Griddle任务在运行则等待
			while(handoffRecycleGriddles.get()) {
				try {
					Thread.sleep(100);
				}
				catch(InterruptedException _) {
					// swallow it
				}
			}
			
			// 最后再做一次Griddle回收
			LOG.info("on stop, recycle griddles for the last time...");
			recycleGriddles();
			
			// 最后Dump一次CBF到硬盘文件
			LOG.info("on stop, dump cbfs to disk files for the last time...");
			dumpCBFsToDisk();
		}
	}

	@Override
	public boolean isRunning() {
		return isRunning.get();
	}

	@Override
	public int getPhase() {
		return -1;
	}


	@Override
	public boolean isAutoStartup() {
		return true;
	}

	@Override
	public void stop(Runnable callback) {
		LOG.info("GriddleManager stop Runnable");
		
		this.stop();
		callback.run();
	}
	
	
	/*
	 * ------------------------------------------------------
	 * Getters/Setters
	 * ------------------------------------------------------
	 */
	
	public String getDumpFileDir() {
		return dumpFileDir;
	}
	
	public long getDumpFileIntervalMillis() {
		return dumpFileIntervalMillis;
	}

	public int getVectorSize() {
		return vectorSize;
	}

	public int getHashType() {
		return hashType;
	}

	public int getHashNum() {
		return hashNum;
	}

	
	/*
	 * ------------------------------------------------------
	 * 对外接口
	 * ------------------------------------------------------
	 */
	
	/**
	 * 由调用方在运行期间动态添加Griddle
	 * @param griddleName Griddle的唯一标识名称
	 * @param maxRepeatInsertCount 最大可重复插入次数
	 */
	public static void addGriddle(String griddleName, int maxRepeatInsertCount) {
		if(StringUtils.isEmpty(griddleName) || maxRepeatInsertCount <= 0) {
			throw new IllegalArgumentException("griddleName should not empty, maxRepeatInsertCount should > 0");
		}
		
		ensureHasStarted();
		if(griddleMap.containsKey(griddleName)) {
			throw new IllegalStateException("griddleMap already contains griddle for name: " + griddleName);
		}
		
		String dumpFileName = buildDumpFileName(griddleName, maxRepeatInsertCount);
		Griddle griddle = Griddle.constructFromGround(maxRepeatInsertCount, vectorSize, hashNum, hashType, 
				dumpFileDir, dumpFileName);
		griddleMap.put(griddleName, griddle);
	}
	
	/**
	 * 运行期间更新Griddle的maxRepeatInsertCount值
	 * @param griddleName
	 * @param newMaxRepeatInsertCount
	 */
	public static void updateMaxRepeatInsertCount(String griddleName, int newMaxRepeatInsertCount) {
		if(StringUtils.isEmpty(griddleName) || newMaxRepeatInsertCount <= 0) {
			throw new IllegalArgumentException("griddleName should not empty, newMaxRepeatInsertCount should > 0");
		}
		
		ensureHasStarted();
		ensureGriddleAlreadyExist(griddleName);
		
		griddleMap.get(griddleName).setMaxRepeatInsertCount(newMaxRepeatInsertCount);
	}
	
	/**
	 * 将某个Griddle内某个关键词的插入次数增1
	 * @param griddleName Griddle唯一标识名称（应用内全局唯一）
	 * @param keyWord 关键词
	 * @return
	 */
	public static boolean increaseInsertCountByOne(String griddleName, String keyWord) {
		if(StringUtils.isEmpty(griddleName) || StringUtils.isEmpty(keyWord)) {
			throw new IllegalArgumentException("gridleName & keyWord should not empty");
		}
		
		ensureHasStarted();
		ensureGriddleAlreadyExist(griddleName);
		
		Griddle griddle = griddleMap.get(griddleName);
		return griddle.add(keyWord);
	}
	
	/**
	 * 获取Griddle内某个关键词已经重复插入的次数
	 * @param griddleName Griddle唯一标识名称（应用内全局唯一）
	 * @param keyWord 关键词
	 * @return 如果参数非法则返回-1，其他情况返回已重复插入次数
	 */
	public static int getHasInsertedCount(String griddleName, String keyWord) {
		if(StringUtils.isEmpty(griddleName) || StringUtils.isEmpty(keyWord)) {
			return -1;
		}
		
		ensureHasStarted();
		ensureGriddleAlreadyExist(griddleName);
		
		Griddle griddle = griddleMap.get(griddleName);
		return griddle.getRepeatedInsertCount(keyWord);
	}
	
	/**
	 * 获取活跃Griddle的名称列表，活跃指该Griddle还没有被回收
	 * @return
	 */
	public static List<String> getActiveGriddleNameList() {
		List<String> activeGriddleNameList = new ArrayList<String> ();
		for(String griddleName: griddleMap.keySet()) {
			Griddle curGriddle = griddleMap.get(griddleName);
			if(!curGriddle.hasRecycled()) {
				activeGriddleNameList.add(griddleName);
			}
		}
		
		return activeGriddleNameList;
	}
	
	/**
	 * 由客户端主动标记“可回收Griddle对象占用的内存以及删除对应的磁盘Dump文件”
	 * @param griddleName Griddle唯一标识名称（应用内全局唯一）
	 */
	public static void markToRecycleGriddle(String griddleName) {
		if(StringUtils.isEmpty(griddleName)) {
			throw new IllegalArgumentException("griddleName should not empty");
		}
		
		LOG.info("mark to recycle griddle: {}", griddleName);
		
		ensureHasStarted();
		ensureGriddleAlreadyExist(griddleName);
		
		Griddle griddle = griddleMap.get(griddleName);
		griddle.markToRecycle();   // 注意是标记Griddle为可回收，而不是立即回收
	}
	
	
	/*
	 * 其它辅助方法
	 */
	
	/**
	 * 确保GriddleManager已启动
	 */
	private static void ensureHasStarted() {
		if (!hasStarted.get()) {
			throw new IllegalStateException("haven't started GriddleManager yet");
		}
	}
	
	/**
	 * 确保Griddle已存在于griddleMap中
	 */
	private static void ensureGriddleAlreadyExist(String griddleName) {
		if(!griddleMap.containsKey(griddleName)) {
			throw new IllegalArgumentException("griddleMap doesn't contains griddle: " + griddleName 
					+ ", you may need use addGriddle to add Griddle to griddleMap");
		}
	}
	
	/**
	 * Dump CBF到磁盘文件
	 */
	private void dumpCBFsToDisk() {
		synchronized (accessDumpFileMutex) {
			for(Griddle griddle: griddleMap.values()) {
				try {
					griddle.dumpCBFToDisk();
				}
				catch(Exception ex) {
					LOG.error("dump CBF to disk file failed: [" + griddle.getDumpFileName() + "]", ex);
				}
			}
		}
	}
	
	/**
	 * 回收所有可以回收的Griddle
	 */
	private void recycleGriddles() {
		handoffRecycleGriddles.set(true);
		
		for(Entry<String, Griddle> entry: griddleMap.entrySet()) {
			Griddle curGriddle = entry.getValue();
			try {
				curGriddle.recycle();
			}
			catch(Exception ex) {
				LOG.error("recycle griddle failed", ex);
			}
		}
		
		List<String> toRemoveGriddleNameList = new ArrayList<String> ();
		for(String griddleName: griddleMap.keySet()) {
			Griddle curGriddle = griddleMap.get(griddleName);
			if(curGriddle.hasRecycled()) {
				toRemoveGriddleNameList.add(griddleName);
			}
		}
		
		for(String toRemoveGriddleName: toRemoveGriddleNameList) {
			griddleMap.remove(toRemoveGriddleName);
		}
		
		handoffRecycleGriddles.set(false);
	}
	
	/**
	 * Dump文件名构成规则：griddleName + "." + maxRepeatInsertCount + ".dat"，比如1.3.dat
	 * @param griddleName
	 * @param maxRepeatInsertCount
	 * @return
	 */
	private static String buildDumpFileName(String griddleName, int maxRepeatInsertCount) {
		StringBuilder dumpFileNameBuilder = new StringBuilder();
		dumpFileNameBuilder.append(griddleName);
		dumpFileNameBuilder.append(".");
		dumpFileNameBuilder.append(maxRepeatInsertCount);
		dumpFileNameBuilder.append(FileUtil.getDumpFileFormatSuffix());
		
		return dumpFileNameBuilder.toString();
	}
	
	/*
	 * ------------------------------------------------------------------
	 * 定时调度任务
	 * ------------------------------------------------------------------
	 */
	
	@Bean
	public ScheduledExecutorFactoryBean scheduledExecutorFactoryBean() {
		ScheduledExecutorFactoryBean scheduledFactoryBean = new ScheduledExecutorFactoryBean();
		scheduledFactoryBean
				.setScheduledExecutorTasks(
					new ScheduledExecutorTask[] { 
						dumpCBFToFileSchedule(),
						recycleGriddleSchedule()
					}
				 );
		
		return scheduledFactoryBean;
	}
	
	@Bean
	public ScheduledExecutorTask dumpCBFToFileSchedule() {
		ScheduledExecutorTask scheduledTask = new ScheduledExecutorTask();
        scheduledTask.setDelay(5000);
        scheduledTask.setPeriod(dumpFileIntervalMillis);
        scheduledTask.setRunnable(new DumpCBFToDiskTask());
        
        return scheduledTask;
	}
	
	@Bean
	public ScheduledExecutorTask recycleGriddleSchedule() {
		ScheduledExecutorTask scheduledTask = new ScheduledExecutorTask();
        scheduledTask.setDelay(2000);
        scheduledTask.setPeriod(recycleGriddleCheckMillis);
        scheduledTask.setRunnable(new RecycleGriddleTask());
        
        return scheduledTask;
	}
	
	/**
	 * 定时Dump内存中的CBF到硬盘文件任务
	 * @author will
	 *
	 */
	private class DumpCBFToDiskTask implements Runnable {

		@Override
		public void run() {
			LOG.info("schedule dump cbfs to disk files...");
			dumpCBFsToDisk();
		}
		
	}
	
	private class RecycleGriddleTask implements Runnable {

		@Override
		public void run() {
			LOG.info("schedule recycle griddles...");
			recycleGriddles();
		}
		
	}
	
}