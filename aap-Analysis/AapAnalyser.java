package com.hadoopexpress.aapanalysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * A class used for anlaysing a sentance based on the positive and negative words list
 * v1.0
 * @author vishnu
 *
 */
public class AapAnalyser {
	
	private static Map<String,Integer> sentimentMap;
	
	public AapAnalyser(String fileName) throws IOException {
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
	
	public int analyse(String sentance) {
		int sentiment = 0;
		String[] words = sentance.split(" ");
		for(String word : words) {
			if (sentimentMap.containsKey(word)) {
				sentiment += sentimentMap.get(word);
			}
		}
		return sentiment;
	}
	
	public static void main(String[] args) throws IOException {
		File f = new File("words");
		AapAnalyser engine = new AapAnalyser("words");
		System.out.println(engine.analyse("This is a good good sentance"));
		System.out.println(engine.analyse("This is a bad sentance"));
	}
	
}
