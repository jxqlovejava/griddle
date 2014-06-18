package com.ximalaya.bloomfilterext.io;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @see <code>Writable</code>
 * @author will
 *
 */
public interface NIOWritable {
	
	  /** 
	   * Serialize the fields of this object to <code>out</code>.
	   * 
	   * @param out <code>DataOuput</code> to serialize this object into.
	   * @throws IOException
	   */
	  void write(ByteBuffer out) throws IOException;

	  /** 
	   * Deserialize the fields of this object from <code>in</code>.  
	   * 
	   * @param in <code>DataInput</code> to deseriablize this object from.
	   * @throws IOException
	   */
	  void readFields(ByteBuffer in) throws IOException;

}
