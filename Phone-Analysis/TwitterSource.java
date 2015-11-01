package com.hadoopexpress.phoneanalysis;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

/**
 * A twitter source to read from Twitter based on the key
 * @author vishnu
 *
 */
public class TwitterSource extends AbstractSource
    implements EventDrivenSource, Configurable {
  
  private String consumerKey;
  private String consumerSecret;
  private String accessToken;
  private String accessTokenSecret;
  
  private String[] searchFor;
  
  private final TwitterStream twitterStream = new TwitterStreamFactory(
      new ConfigurationBuilder()
        .setJSONStoreEnabled(true)
        .build()).getInstance();

  /**
   * Configures the keys required for accessing Twitter. Also gets the productKeys 
   * */
  @Override
  public void configure(Context context) {
    consumerKey = context.getString("consumerKey");
    consumerSecret = context.getString("consumerSecret");
    accessToken = context.getString("accessToken");
    accessTokenSecret = context.getString("accessTokenSecret");
    
    String prodctKeys = context.getString("Product", "");
    searchFor = prodctKeys.split(",");
    for (int i = 0; i < searchFor.length; i++) {
    	searchFor[i] = searchFor[i].trim();
    }
  }

  /**
   * Starts listening for twitter events 
   */
  @Override
  public void start() {
    // The channel connects from source to sink 
    final ChannelProcessor channel = getChannelProcessor();
    
    final Map<String, String> headers = new HashMap<String, String>();
    
    // The method onStatus() will be called everytime a tweet comes.
    StatusListener listener = new StatusListener() {
    	//capture new tweet notification
      public void onStatus(Status status) {
    	//add creation time in the header
        headers.put("timestamp", String.valueOf(status.getCreatedAt().getTime()));
        Event event = EventBuilder.withBody(
            DataObjectFactory.getRawJSON(status).getBytes(), headers);
        //send to sink
        channel.processEvent(event);
      }
      
      //ignore all other notifications
      public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
      public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
      public void onScrubGeo(long userId, long upToStatusId) {}
      public void onException(Exception ex) {}

	@Override
	public void onStallWarning(StallWarning arg0) {
		//do nothing
	}
    };
    
    //add the listener we created + tell the stream all about the security info required
    twitterStream.addListener(listener);
    twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
    AccessToken token = new AccessToken(accessToken, accessTokenSecret);
    twitterStream.setOAuthAccessToken(token);
    
    // Set up a filter to pull out industry-relevant tweets
    if (searchFor.length == 0) {
      System.out.println("Please setup filter keyword in Flume conf");
    } else {
    //create a filter query for filtering out only the required info
      FilterQuery query = new FilterQuery().track(searchFor);
      twitterStream.filter(query);
    }
    super.start();
  }
  
  /**
   * What happens when the stream is shutdown
   */
  @Override
  public void stop() {
    System.out.println("Shutting down");
    twitterStream.shutdown();
    super.stop();
  }
}