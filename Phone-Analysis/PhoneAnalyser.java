package com.hadoopexpress.phoneanalysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A class used for anlaysing a sentance based on the positive and negative words list
 * v1.0
 * @author vishnu
 *
 */
public class PhoneAnalyser {
	
	private static Map<String,Integer> sentimentMap;
	
	public PhoneAnalyser(String fileName) throws IOException {
		sentimentMap = new HashMap<String,Integer>();
		InputStream is = this.getClass().getResourceAsStream(fileName);
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		String line = null;
		while((line = br.readLine()) !=  null) {
			String[] entries = line.split(" ");
			if (entries.length != 2)
				continue;
			String word = entries[0];
			Integer value = Integer.parseInt(entries[1]);
			sentimentMap.put(word,value);	
		}
		br.close();
		is.close();
	}
	
	public String[] analyse(String sentance) {
		boolean iphone = false;
		boolean s4 = false;
		boolean htc = false;
		int sentiment = 0;
		String sentanceUpper = sentance.toUpperCase();
		
		if (sentanceUpper.contains("SAMSUNG") || sentanceUpper.contains("S4")) {
			s4 = true;
		}else if (sentanceUpper.contains("IPHONE5S") || sentanceUpper.contains("5S")) {
			iphone = true;
		} else if (sentanceUpper.contains("HTC")) {
			htc = true;
		}
		
		String[] words = sentance.split(" ");
		for(String word : words) {
			if (sentimentMap.containsKey(word)) {
				sentiment += sentimentMap.get(word);
			}
		}
		String key = "";
		if (iphone) {
			key = "iphone5s";
			return new String[]{key,sentiment+""};
		}
		else if (s4) {
			key = "s4";
			return new String[]{key,sentiment+""};
		}
		else if(htc) {
			key = "htc";
			return new String[]{key,sentiment+""};
		}
		
		key = "ignore";
		return new String[]{key,sentiment+""};
		
	}
	
	public static void main(String[] args) throws IOException {
		File f = new File("words");
		PhoneAnalyser engine = new PhoneAnalyser("words");
		System.out.println(engine.analyse("This is a good good sentance"));
		System.out.println(engine.analyse("This is a bad sentance"));
	}
	
}
