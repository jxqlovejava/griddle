package com.ximalaya.bloomfilterext.bloom;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

public class TestDataOutput {
	
	@Test
	public void testDataOutput() throws IOException {
		File file = new File("e:/test/test.txt");
		if(file.exists()) {
			file.delete();
		}
		
		FileOutputStream fos = new FileOutputStream(file);
		DataOutputStream dos = new DataOutputStream(fos);
		dos.writeLong(4);
		dos.close();
		fos.close();
		
		FileInputStream fis = new FileInputStream(file);
		DataInputStream dis = new DataInputStream(fis);
		long readResult = dis.readLong();
		dis.close();
		fis.close();
		
		Assert.assertTrue(readResult == 4);
	}
  
}
