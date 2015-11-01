package com.hadoopexpress.phoneanalysis;

/**
 * A custom Key type corresponding to a rectangle
 * 
 *    B-------------------------C
 *    |							|
 *    |							|
 *    |							|
 *    |							|
 *    A-------------------------D
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class SentimentTuple implements Writable{
	
	public String item;
	public int sentiment;
	
	
	public SentimentTuple(String item,int sentiment) {
		this.item = item;
		this.sentiment = sentiment;
	}
	
	public SentimentTuple() {
		this.item = "";
		this.sentiment = 0;
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		System.out.println("Called read on SentimentTuple");
		item = in.readUTF();
		System.out.println("Read item"+item);
		sentiment = in.readInt();
		System.out.println("Read sentiment"+sentiment);
		System.out.println("--------");		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		System.out.println("Called write on SentimentTuple");
		System.out.println("item "+item);
		System.out.println("sentiment "+sentiment);		
		System.out.println("--------------------");
		out.writeUTF(item);
		out.writeInt(sentiment);
		
	}

	
	@Override 
	public String toString() {
		return item+" "+sentiment;
	}

}
