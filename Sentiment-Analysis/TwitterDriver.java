package com.hadoopexpress.sentimentanalysis;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
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
import org.apache.hadoop.mapred.lib.IdentityReducer;


public class TwitterDriver {
	static SentimentAnalyser analyser = null;
	static {
		 
		try {
			analyser = new SentimentAnalyser("words");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static class TwitterMapper extends MapReduceBase implements Mapper<LongWritable,Text,IntWritable,IntWritable>{

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String lang = getTagValue("lang",line);
			int sentiment = 0;
			if (lang.equals("en")) {
				String tweet = getTagValue("text",line);
				System.out.println("Tweet : "+tweet);
				sentiment = analyser.analyse(tweet);
				System.out.println(sentiment);
			}
			String createdAt = getTagValue("created_at",line);
			try {
				int hour = getHour(createdAt);
				output.collect(new IntWritable(hour),new IntWritable(sentiment));
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			
		}
		
	}
	
	public static class TwitterReducer extends MapReduceBase implements Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

		@Override
		public void reduce(IntWritable key, Iterator<IntWritable> values,
				OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
				throws IOException {
			
			
			int sum = 0;
			System.out.println("In reducer");
			System.out.println(key.toString());
			while(values.hasNext()) {
				IntWritable value = values.next();
				System.out.println(value.toString());
				sum += value.get();
			}
			output.collect(key,new IntWritable(sum));
		}
		
	}
	
	public static void main(String[] args) throws IOException, ParseException {
		
		
		JobConf conf = new JobConf(TwitterDriver.class);
		conf.setJobName("ProdAnalysis");

	    conf.setOutputKeyClass(IntWritable.class);
	    conf.setOutputValueClass(IntWritable.class);

	    conf.setMapperClass(TwitterMapper.class);
	    conf.setReducerClass(TwitterReducer.class);

	    conf.setInputFormat(TextInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);

	    FileInputFormat.setInputPaths(conf, new Path(args[0]));
	    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	    JobClient.runJob(conf);
		
		/*String temp = "Thu Dec 23 00:11:07 +0000 2010";
		System.out.println(getMinute(temp));*/
	}
	
	public static String getTagValue(String tag,String line) {
		int startIndex = line.indexOf(tag);
		if (startIndex == -1)
			return "{Tag not found}";
		startIndex = startIndex + tag.length()+3;
		String temp = line.substring(startIndex);
		return temp.substring(0,temp.indexOf("\""));
		
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
