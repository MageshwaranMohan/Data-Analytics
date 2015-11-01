package com.hadoopexpress.phoneanalysis;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class PhoneDriver {
	static PhoneAnalyser analyser = null;
	static {

		try {
			analyser = new PhoneAnalyser("words");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static class TwitterMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, SentimentTuple> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, SentimentTuple> output,
				Reporter reporter) throws IOException {
			String line = value.toString();
			String lang = getTagValue("lang", line);
			String[] result = new String[2];
			if (lang.equals("en")) {
				String tweet = getTagValue("text", line);
				System.out.println("Tweet : " + tweet);
				result = analyser.analyse(tweet);

				String createdAt = getTagValue("created_at", line);
				SentimentTuple sentTuple = new SentimentTuple();
				sentTuple.item = result[0];
				if (sentTuple.item.equals("ignore"))
					return;
				sentTuple.sentiment = Integer.parseInt(result[1]);
				try {
					int hour = getHour(createdAt);
					output.collect(new IntWritable(hour), sentTuple);
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}

		}

	}

	public static class TwitterReducer extends MapReduceBase implements
			Reducer<IntWritable, SentimentTuple, IntWritable, Text> {

		@Override
		public void reduce(IntWritable key, Iterator<SentimentTuple> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {

			Map<String, Integer> sumMap = new HashMap<String, Integer>();
			System.out.println("In reducer");
			System.out.println(key.toString());
			while (values.hasNext()) {
				SentimentTuple value = values.next();
				String product = value.item;
				int sentiment = value.sentiment;
				if (sumMap.containsKey(product)) {
					int curr = sumMap.get(product);
					curr += sentiment;
					sumMap.put(product, curr);
				} else {
					sumMap.put(product, sentiment);
				}

			}
			String combinedValue = "";
			for (String prodKey : sumMap.keySet()) {
				System.out.println("prodKey "+prodKey);
				int prodValue = sumMap.get(prodKey);
				System.out.println("Sentiment "+prodValue);
				combinedValue += prodKey + " " + prodValue + ", ";
			}
			System.out.println("Combined value is "+combinedValue);
			if (!combinedValue.equals(""))
				output.collect(key, new Text(combinedValue));

		}

	}

	public static void main(String[] args) throws IOException, ParseException {

		JobConf conf = new JobConf(PhoneDriver.class);
		conf.setJobName("ProdAnalysis");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(SentimentTuple.class);

		conf.setMapperClass(TwitterMapper.class);
		conf.setReducerClass(TwitterReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);

		/*
		 * String temp = "Thu Dec 23 00:11:07 +0000 2010";
		 * System.out.println(getMinute(temp));
		 */
	}

	public static String getTagValue(String tag, String line) {
		int startIndex = line.indexOf(tag);
		if (startIndex == -1)
			return "{Tag not found}";
		startIndex = startIndex + tag.length() + 3;
		String temp = line.substring(startIndex);
		return temp.substring(0, temp.indexOf("\""));

	}

	public static int getHour(String date) throws ParseException {

		final String TWITTER = "EEE MMM dd HH:mm:ss zzzzz yyyy";
		SimpleDateFormat sf = new SimpleDateFormat(TWITTER);
		sf.setLenient(true);
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		Calendar c = Calendar.getInstance();
		Date d = sf.parse(date);
		c.setTime(d);
		return c.get(Calendar.HOUR_OF_DAY);
	}

}
