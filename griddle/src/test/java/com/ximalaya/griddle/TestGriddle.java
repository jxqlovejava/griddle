package com.ximalaya.griddle;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.transaction.TransactionConfiguration;

@ContextConfiguration(locations="classpath:application-context.xml")
@RunWith(SpringJUnit4ClassRunner.class)
@TransactionConfiguration(transactionManager="txManager", defaultRollback=false)
public class TestGriddle {
	
	@Test
	public void testAddGriddle() {
		GriddleManager.addGriddle("1", 3);
		GriddleManager.addGriddle("2", 5);
		GriddleManager.addGriddle("3", 5);
	}
	
	@Test
	public void testIncreaseInsertCountByOne() {
	/*	Assert.isTrue(GriddleManager.increaseInsertCountByOne("1", "toupiao:1:1001"));
		Assert.isTrue(GriddleManager.increaseInsertCountByOne("1", "toupiao:1:1001"));
		Assert.isTrue(GriddleManager.increaseInsertCountByOne("1", "toupiao:1:1001"));
		Assert.isTrue(!GriddleManager.increaseInsertCountByOne("1", "toupiao:1:1001"));*/
		
//		Assert.isTrue(!GriddleManager.increaseInsertCountByOne("1", "toupiao:1:1001"));
		
		/*Assert.isTrue(GriddleManager.increaseInsertCountByOne("2", "toupiao:2:1001"));
		Assert.isTrue(GriddleManager.increaseInsertCountByOne("2", "toupiao:2:1001"));
		Assert.isTrue(GriddleManager.increaseInsertCountByOne("2", "toupiao:2:1001"));
		Assert.isTrue(GriddleManager.increaseInsertCountByOne("2", "toupiao:2:1001"));
		Assert.isTrue(GriddleManager.increaseInsertCountByOne("2", "toupiao:2:1001"));
		Assert.isTrue(!GriddleManager.increaseInsertCountByOne("2", "toupiao:2:1001"));*/
		
//		Assert.isTrue(!GriddleManager.increaseInsertCountByOne("2", "toupiao:2:1001"));
	}
	
	@Test
	public void testMarkToRecycleGriddle() {
//		GriddleManager.markToRecycleGriddle("2");
	}
	
	@Test
	public void sleep() {
		try {
			Thread.sleep(3000);
		}
		catch(InterruptedException _) {
			// 
		}
	}

}
