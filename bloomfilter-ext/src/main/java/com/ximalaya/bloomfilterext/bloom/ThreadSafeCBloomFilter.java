package com.ximalaya.bloomfilterext.bloom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLongArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 线程安全的CountingBloomFilter
 * @author will
 *
 */
public final class ThreadSafeCBloomFilter extends Filter {
	
	 /** Storage for the counting buckets */
	  private AtomicLongArray buckets;

	  /** We are using 4bit buckets, so each bucket can count to 15 */
	  private final static long BUCKET_MAX_VALUE = 15;
	  
	  /** Max update retry times */
	  private final static long MAX_UPDATE_RETRY_TIMES = 6;
	  
	  private static final Logger LOG = LoggerFactory.getLogger(AdjustedCountingBloomFilter.class);
	  
	  /** Default constructor - use with readFields */
	  public ThreadSafeCBloomFilter() {}
	  
	  /**
	   * Constructor
	   * @param vectorSize The vector size of <i>this</i> filter.
	   * @param nbHash The number of hash function to consider.
	   * @param hashType type of the hashing function (see
	   * {@link org.apache.hadoop.util.hash.Hash}).
	   */
	  public ThreadSafeCBloomFilter(int vectorSize, int nbHash, int hashType) {
	    super(vectorSize, nbHash, hashType);
	    int bucketSize = buckets2words(vectorSize);
	    buckets = new AtomicLongArray(bucketSize);
	  }

	  /** returns the number of 64 bit words it would take to hold vectorSize buckets */
	  private static int buckets2words(int vectorSize) {
	   return ((vectorSize - 1) >>> 4) + 1;
	  }


	  @Override
	  public void add(Key key) {
	    if(key == null) {
	      throw new NullPointerException("key can not be null");
	    }

	    int[] h = hash.hash(key);
	    hash.clear();

	    for(int i = 0; i < nbHash; i++) {
	      // find the bucket
	      int wordNum = h[i] >> 4;          // div 16
	      int bucketShift = (h[i] & 0x0f) << 2;  // (mod 16) * 4
	      
	      long bucketMask = 15L << bucketShift;
	      
	      boolean hasUpdatedSuccess = false;   // 是否更新成功
	      int retriedTimes = 0;   // 已重试次数
	      while(!hasUpdatedSuccess && retriedTimes < MAX_UPDATE_RETRY_TIMES) {
	    	  long oldVal = buckets.get(wordNum);
		      long bucketValue = (oldVal & bucketMask) >>> bucketShift;
		      
		      // only increment if the count in the bucket is less than BUCKET_MAX_VALUE
		      if(bucketValue < BUCKET_MAX_VALUE) {
		        // increment by 1
		        hasUpdatedSuccess = buckets.compareAndSet(wordNum,
		        										  oldVal, 
		        									      (oldVal & ~bucketMask) | ((bucketValue + 1) << bucketShift));
		      }
		      
		      retriedTimes++;
	      }
	      
	      // do log
	      if(!hasUpdatedSuccess && retriedTimes == MAX_UPDATE_RETRY_TIMES) {
	    	  LOG.error("collision occurn: add");
	      }
	    }
	  }

	  /**
	   * Removes a specified key from <i>this</i> counting Bloom filter.
	   * <p>
	   * <b>Invariant</b>: nothing happens if the specified key does not belong to <i>this</i> counter Bloom filter.
	   * @param key The key to remove.
	   */
	  public void delete(Key key) {
	    if(key == null) {
	      throw new NullPointerException("Key may not be null");
	    }
	    if(!membershipTest(key)) {
	      throw new IllegalArgumentException("Key is not a member");
	    }

	    int[] h = hash.hash(key);
	    hash.clear();

	    for(int i = 0; i < nbHash; i++) {
	      // find the bucket
	      int wordNum = h[i] >> 4;          // div 16
	      int bucketShift = (h[i] & 0x0f) << 2;  // (mod 16) * 4
	      
	      long bucketMask = 15L << bucketShift;
	      
	      boolean hasUpdatedSuccess = false;   // 是否更新成功
	      int retriedTimes = 0;   // 已重试次数
	      while(!hasUpdatedSuccess && retriedTimes < MAX_UPDATE_RETRY_TIMES) {
	    	  long oldVal = buckets.get(wordNum);
		      long bucketValue = (oldVal & bucketMask) >>> bucketShift;
		      // only decrement if the count in the bucket is between 0 and BUCKET_MAX_VALUE
		      if(bucketValue >= 1 && bucketValue < BUCKET_MAX_VALUE) {
		        // decrement by 1
		        hasUpdatedSuccess = buckets.compareAndSet(wordNum,
		        										  oldVal, 
		        										  (oldVal & ~bucketMask) | ((bucketValue - 1) << bucketShift));
		      }
		      
		      retriedTimes++;
	      }   // while ends
	      
	      // do log
	      if(!hasUpdatedSuccess && retriedTimes == MAX_UPDATE_RETRY_TIMES) {
	    	  LOG.error("collision occurn: delete");
	      }
	    }
	  }

	  @Override
	  public void and(Filter filter) {
	    if(filter == null
	        || !(filter instanceof ThreadSafeCBloomFilter)
	        || filter.vectorSize != this.vectorSize
	        || filter.nbHash != this.nbHash) {
	      throw new IllegalArgumentException("filters cannot be and-ed");
	    }
	    ThreadSafeCBloomFilter cbf = (ThreadSafeCBloomFilter) filter;
	    
	    int sizeInWords = buckets2words(vectorSize);
	    for(int i = 0; i < sizeInWords; i++) {
	    	boolean hasUpdatedSuccess = false;   // 是否更新成功
	    	int retriedTimes = 0;   // 已重试次数
	    	while(!hasUpdatedSuccess && retriedTimes < MAX_UPDATE_RETRY_TIMES) {
	    		long oldVal = buckets.get(i);
	    		hasUpdatedSuccess = buckets.compareAndSet(i, oldVal, oldVal & cbf.buckets.get(i));
	    	  
	    		retriedTimes++;
	    	}   // while ends
	    	
		    // do log
		    if(!hasUpdatedSuccess && retriedTimes == MAX_UPDATE_RETRY_TIMES) {
		        LOG.error("collision occurn: and");
		    }
	    }
	  }

	  @Override
	  public boolean membershipTest(Key key) {
	    if(key == null) {
	      throw new NullPointerException("Key may not be null");
	    }

	    int[] h = hash.hash(key);
	    hash.clear();

	    for(int i = 0; i < nbHash; i++) {
	      // find the bucket
	      int wordNum = h[i] >> 4;          // div 16
	      int bucketShift = (h[i] & 0x0f) << 2;  // (mod 16) * 4

	      long bucketMask = 15L << bucketShift;

	      if((buckets.get(wordNum) & bucketMask) == 0) {
	        return false;
	      }
	    }

	    return true;
	  }

	  /**
	   * This method calculates an approximate count of the key, i.e. how many
	   * times the key was added to the filter. This allows the filter to be
	   * used as an approximate <code>key -&gt; count</code> map.
	   * <p>NOTE: due to the bucket size of this filter, inserting the same
	   * key more than 15 times will cause an overflow at all filter positions
	   * associated with this key, and it will significantly increase the error
	   * rate for this and other keys. For this reason the filter can only be
	   * used to store small count values <code>0 &lt;= N &lt;&lt; 15</code>.
	   * @param key key to be tested
	   * @return 0 if the key is not present. Otherwise, a positive value v will
	   * be returned such that <code>v == count</code> with probability equal to the
	   * error rate of this filter, and <code>v &gt; count</code> otherwise.
	   * Additionally, if the filter experienced an underflow as a result of
	   * {@link #delete(Key)} operation, the return value may be lower than the
	   * <code>count</code> with the probability of the false negative rate of such
	   * filter.
	   */
	  public int approximateCount(Key key) {
	    int res = Integer.MAX_VALUE;
	    int[] h = hash.hash(key);
	    hash.clear();
	    for (int i = 0; i < nbHash; i++) {
	      // find the bucket
	      int wordNum = h[i] >> 4;          // div 16
	      int bucketShift = (h[i] & 0x0f) << 2;  // (mod 16) * 4
	      
	      long bucketMask = 15L << bucketShift;
	      long bucketValue = (buckets.get(wordNum) & bucketMask) >>> bucketShift;
	      if (bucketValue < res) 
	    	  res = (int)bucketValue;
	    }
	    
	    if (res != Integer.MAX_VALUE) {
	      return res;
	    } else {
	      return 0;
	    }
	  }

	  @Override
	  public void not() {
	    throw new UnsupportedOperationException("not() is undefined for "
	        + this.getClass().getName());
	  }

	  @Override
	  public void or(Filter filter) {
	    if(filter == null
	        || !(filter instanceof Filter)
	        || filter.vectorSize != this.vectorSize
	        || filter.nbHash != this.nbHash) {
	      throw new IllegalArgumentException("filters cannot be or-ed");
	    }

	    ThreadSafeCBloomFilter cbf = (ThreadSafeCBloomFilter) filter;

	    int sizeInWords = buckets2words(vectorSize);
	    for(int i = 0; i < sizeInWords; i++) {
	    	boolean hasUpdatedSuccess = false;   // 是否更新成功
	    	int retriedTimes = 0;   // 已重试次数
	    	while(!hasUpdatedSuccess && retriedTimes < MAX_UPDATE_RETRY_TIMES) {
	    		long oldVal = buckets.get(i);
	    		hasUpdatedSuccess = buckets.compareAndSet(i, oldVal, oldVal | cbf.buckets.get(i));
	    		
	    		retriedTimes++;
	    	}   // while ends
	    	
		    // do log
		    if(!hasUpdatedSuccess && retriedTimes == MAX_UPDATE_RETRY_TIMES) {
		    	LOG.error("collision occurn: or");
		    }
	    }
	  }

	  @Override
	  public void xor(Filter filter) {
	    throw new UnsupportedOperationException("xor() is undefined for "
	        + this.getClass().getName());
	  }

	  @Override
	  public void clear() {
		  this.buckets = null;
	  }
	  
	  @Override
	  public String toString() {
	    StringBuilder res = new StringBuilder();

	    for(int i = 0; i < vectorSize; i++) {
	      if(i > 0) {
	        res.append(" ");
	      }
	      
	      int wordNum = i >> 4;          // div 16
	      int bucketShift = (i & 0x0f) << 2;  // (mod 16) * 4
	      
	      long bucketMask = 15L << bucketShift;
	      long bucketValue = (buckets.get(wordNum) & bucketMask) >>> bucketShift;
	      
	      res.append(bucketValue);
	    }

	    return res.toString();
	  }

	  public int getVectorSize() {
		  return this.vectorSize;
	  }
	  
	  public int getNbHash() {
		  return this.nbHash;
	  }
	  
	  public int getHashType() {
		  return this.hashType;
	  }
	  
	  // Writable

	  @Override
	  public void write(DataOutput out) throws IOException {
	    super.write(out);
	    int sizeInWords = buckets2words(vectorSize);
	    for(int i = 0; i < sizeInWords; i++) {
	      out.writeLong(buckets.get(i));
	    }
	  }

	  @Override
	  public void readFields(DataInput in) throws IOException {
	    super.readFields(in);
	    int sizeInWords = buckets2words(vectorSize);
	    buckets = new AtomicLongArray(sizeInWords);
	    for(int i = 0; i < sizeInWords; i++) {
	    	buckets.set(i, in.readLong());
	    }
	  }

}
