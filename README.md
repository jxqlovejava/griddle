##注意：
* 发现Hadoop的CountingBloomFilter实现是非线程安全的，TODO：替换成线程安全的CountingBloomFilter实现
* CountingBloomFilter最大可重复插入次数必须小于等于15

##griddle包含项目

griddle是一个简单的轻量级通用组件。它包含以下两个项目：

* bloomfilter-ext：提取自Hadoop源码，自己只扩展了一个AdjustedCountingBloomFilter类，修改其Dump方式为使用NIO的ByteBuffer
* griddle：依赖bloomfilter-ext项目，包含该通用组件的具体实现

##适合场景和特性
比如现在很多网站会发起一些投票活动，但需要限制每个用户投票总数，如果网站用户数量级比较大并且投票活动比较傲频繁时，我们该如何存储用户剩余可投票次数呢？

* 直接存储在MySQL这种关系型数据库中？不靠谱，查询和更新太耗时
* 存放在Redis这种KV数据库中？大部分情况可以，但比较耗内存，并且Redis基于TCP/IP网络协议，有网络连接开销

大数据算法中有一个叫Bloom Filter（中文名叫布隆过滤器）的算法，它能通过牺牲一定准确性来换取大量内存空间节省，并且由于操作是在内存中，非常适用于某些业务场景，比如爬虫的URL去重等。Bloom Filter的一个扩展算法是Counting Bloom Filter，它相比Bloom Filter支持删除，而且可以统计某个Key已重复插入的次数。更多关于Bloom Filter和Count Bloom Filter算法原理和实现细节，可以参考网络上的其他资料和查看bloomfilter-ext项目源码。

griddle正是基于Counting Bloom Filter实现的。此外，它还包含以下扩展特性：

* 程序运行过程会定时Dump内存中的Counting Bloom Filter数据结构到磁盘，这样在应用意外崩溃后再次启动时，会从Dump文件恢复Counting Bloom Filter为崩溃前状态
* 会定时回收满足回收条件的Griddle对象（它内部封装了一个Counting Bloom Filter和Dump文件相关信息）

##使用方法
###添加Maven依赖
首先通过在你项目的Maven的pom.xml中添加以下依赖关系：

```xml
<dependency>
    <groupId>com.ximalaya</groupId>
	<artifactId>bloomfilter-ext</artifactId>
	<version>0.0.1-SNAPSHOT</version>
</dependency>
<dependency>
	<groupId>com.ximalaya</groupId>
	<artifactId>griddle</artifactId>
	<version>0.0.1-SNAPSHOT</version>
</dependency>
```
###增加griddle-config.properties配置文件
```properties
griddle.config.dumpFileDir=/usr/local/dump
griddle.config.dumpFileIntervalMillis=5000
griddle.config.recycleGriddleCheckMillis=1000
griddle.config.vectorSize=100000
griddle.config.hashType=1
griddle.config.hashNum=20
```

上面的参数说明如下：
<table>
    <tr>
        <td>属性</td>
        <td>描述</td>
    </tr>
    <tr>
    	<td>dumpFileDir</td>
    	<td>Dump文件存放目录</td>
    </tr>
    <tr>
    	<td>dumpFileIntervalMillis</td>
    	<td>Dump文件时间间隔，单位毫秒</td>
    </tr>
    <tr>
    	<td>recycleGriddleCheckMillis</td>
    	<td>定时回收Griddle时间间隔，单位毫秒</td>
    </tr>
    <tr>
    	<td>vectorSize</td>
    	<td>预估的unique key数目</td>
    </tr>
    <tr>
    	<td>hashType</td>
    	<td>Counting Bloom Filter使用的哈希函数，1为MurMurHash，0为JekinHash，建议维持为1</td>
    </tr>
    <tr>
    	<td>hashNum</td>
    	<td>每个key映射到Counting Bloom Filter数据结构的多少位，一般这个值越大，误判概率越低</td>
    </tr>
</table>

###配置application-context.xml
只需要在application-context.xml中配置一个GriddleManager的Bean即可：
```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://www.springframework.org/schema/beans"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:context="http://www.springframework.org/schema/context"
	xmlns:aop="http://www.springframework.org/schema/aop" xmlns:tx="http://www.springframework.org/schema/tx"
	xmlns:util="http://www.springframework.org/schema/util"
	xsi:schemaLocation="http://www.springframework.org/schema/beans
		http://www.springframework.org/schema/beans/spring-beans-3.0.xsd
		http://www.springframework.org/schema/aop
		http://www.springframework.org/schema/aop/spring-aop-3.0.xsd
		http://www.springframework.org/schema/context
		http://www.springframework.org/schema/context/spring-context-3.0.xsd
		http://www.springframework.org/schema/tx
		http://www.springframework.org/schema/tx/spring-tx-3.0.xsd
		http://www.springframework.org/schema/util
		http://www.springframework.org/schema/util/spring-util-3.0.xsd
		"
	default-lazy-init="false">
	
	<bean id="propertyConfigurer"
		class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer">
		<property name="locations">
			<list>
				<value>classpath:griddle-config.properties</value>
			</list>
		</property>
	</bean>
	
	<!-- Griddle管理器Bean -->
	<bean id="griddleManager" class="com.ximalaya.griddle.GriddleManager">
		<constructor-arg index="0" value="${griddle.config.dumpFileDir}" />
		<constructor-arg index="1" value="${griddle.config.dumpFileIntervalMillis}" />
		<constructor-arg index="2" value="${griddle.config.recycleGriddleCheckMillis}" />
		<constructor-arg index="3" value="${griddle.config.vectorSize}" />
		<constructor-arg index="4" value="${griddle.config.hashType}" />
		<constructor-arg index="5" value="${griddle.config.hashNum}" />
	</bean>
	
</beans>
```

###使用API接口
griddle提供了五个接口，分别如下（前三个最常用）：

* public static void addGriddle(String griddleName, int maxRepeatInsertCount)：添加一个Griddle对象到GriddleManager中，交由griddle框架管理：第一个参数为griddle的唯一标识名(<strong>注意不能包含英文句点</strong>)，必须在应用内唯一；第二个参数设定可重复插入Counting Bloom Filter次数。如果是投票数限制场景，那就是某个活动每个用户投票数上限值。比如：

```java
GriddleManager.addGriddle("toupiao1", 3);
```

* boolean increaseInsertCountByOne(String griddleName, String keyWord)：将某个Griddle中某个Key的插入次数增加一。以投票数限制场景为例，代码如下，相当于用户1001在toupiao1活动中投票数加1，如果返回true说明满足投票条件（即还没有达到最大次数3限制），反之返回false表示他之前已用尽了投票次数：

```java
GriddleManager.increaseInsertCountByOne("toupiao1", "1001"));
```

* public static void markToRecycleGriddle(String griddleName)：标记某个名称为griddleName的Griddle可以被回收了。后台定时任务会轮询所有Griddle对象，当同时满足Griddle对象已被标记为可以回收并且使用该Griddle对象的计数为0，则释放Griddle对象占用的内存并删除对应的磁盘Dump文件

* public static void updateMaxRepeatInsertCount(String griddleName, int newMaxRepeatInsertCount)：运行期间更新某个Griddle的最大可重复插入次数

* public static List&lt;String&gt; getActiveGriddleNameList()：获取活跃Griddle的名称列表，活跃指该Griddle还没有被真正回收

在你的代码中你只需要组合使用这几个接口就好了。比如：
```java
String uniqueGriddleName = "toupiao1";   // 投票活动名用作Griddle唯一标识名
GriddleManager.addGriddle(uniqueGriddleName, 3);   // 添加一个投票活动对应的Griddle


if(GriddleManager.increaseInsertCountByOne(uniqueGriddleName, "1001")) {
   // 用户1001还有剩余投票次数
   // TODO 用户投票
}
else {
   // 用户1001已达到投票次数上限，不能进行投票
}
```

整个组件就是如此简单，欢迎大家提意见或者发表看法，我的Email是：jxqlovezlj@gmail.com
