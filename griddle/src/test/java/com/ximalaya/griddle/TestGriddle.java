package com.ximalaya.griddle;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.Assert;

import com.ximalaya.griddle.GriddleManager;

@ContextConfiguration(locations="classpath:application-context.xml")
@RunWith(SpringJUnit4ClassRunner.class)
public class TestGriddle {
	
	@Test
	public void testAddGriddle() {
		GriddleManager.addGriddle("1", 3);
	}
	
	@Test
	public void testIncreaseInsertCountByOne() {
		Assert.isTrue(GriddleManager.increaseInsertCountByOne("1", "toupiao:1:1001"));
		Assert.isTrue(GriddleManager.increaseInsertCountByOne("1", "toupiao:1:1001"));
		Assert.isTrue(GriddleManager.increaseInsertCountByOne("1", "toupiao:1:1001"));
		Assert.isTrue(!GriddleManager.increaseInsertCountByOne("1", "toupiao:1:1001"));
	}
	
	@Test
	public void testMarkToRecycleGriddle() {
		GriddleManager.markToRecycleGriddle("1");
	}

}
