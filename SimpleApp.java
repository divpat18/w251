/* SimpleApp.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.*;
import twitter4j.*;
import java.util.Arrays;
import scala.Tuple2;
import java.util.*;
import org.apache.spark.Logging;
//import org.apache.log4j.*;

public class SimpleApp {
 public static void main(String[] args) {
  String consumerKey = args[0];
  String consumerSecret = args[1];
  String accessToken = args[2];
  String accessTokenSecret = args[3];


  System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
  System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
  System.setProperty("twitter4j.oauth.accessToken", accessToken);
  System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

  SparkConf conf = new SparkConf().setAppName("Simple Application");
  JavaStreamingContext sc = new JavaStreamingContext(new JavaSparkContext(conf), Durations.seconds(10));

  JavaReceiverInputDStream < Status > stream = TwitterUtils.createStream(sc);
/* SimpleApp.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.*;
import twitter4j.*;
import java.util.Arrays;
import scala.Tuple2;
import java.util.*;
import org.apache.spark.Logging;
//import org.apache.log4j.*;

public class SimpleApp {
 public static void main(String[] args) {
  String consumerKey = args[0];
  String consumerSecret = args[1];
  String accessToken = args[2];
  String accessTokenSecret = args[3];


  System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
  System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
  System.setProperty("twitter4j.oauth.accessToken", accessToken);
  System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

  SparkConf conf = new SparkConf().setAppName("Simple Application");
  JavaStreamingContext sc = new JavaStreamingContext(new JavaSparkContext(conf), Durations.seconds(10));

  JavaReceiverInputDStream < Status > stream = TwitterUtils.createStream(sc);

  JavaDStream < RawTweet > rawTweets = stream.map(new Function < Status, RawTweet > () {
   public RawTweet call(Status status) {
    RawTweet rt = new RawTweet(status.getUser().getScreenName(), status.getText());
    return rt;
   }
  });

  JavaDStream < Tweet > tweets = rawTweets.flatMap(
   new FlatMapFunction < RawTweet, Tweet > () {
    public Iterable < Tweet > call(RawTweet rt) {
     List < String > words = Arrays.asList(rt.text.split(" "));
     List < Tweet > tw = new ArrayList < Tweet > ();
     List < String > tags = new ArrayList < String > ();
     List < String > mention = new ArrayList < String > ();
     for (String word: words) {
      if (word.startsWith("#")) {
       tags.add(word);
      } else if (word.startsWith("@")) {
       mention.add(word);
      }
     }

     for (String tag: tags) {
      tw.add(new Tweet(tag, rt.user, mention));
     }
     return tw;
    }
   }
  );

  JavaPairDStream < String, Tweet > tagTweetPair = tweets.mapToPair(
   new PairFunction < Tweet, String, Tweet > () {
    public Tuple2 < String, Tweet > call(Tweet t) {
     return new Tuple2 < String, Tweet > (t.hashTag, t);
    }
   }
  );

  JavaPairDStream < String, Tweet > reducedTagTweetPair = tagTweetPair.reduceByKeyAndWindow(
   new Function2 < Tweet, Tweet, Tweet > () {
    public Tweet call(Tweet t1, Tweet t2) {
     String combinedAuth = t1.author + "," + t2.author;
     t1.getMentions().addAll(t2.getMentions());
     Tweet result = new Tweet(t1.hashTag, combinedAuth, t1.getMentions());
     return result;
    }
   },
   new Duration(60 * 5 * 1000),
   new Duration(10 * 1000)
  );


  JavaPairDStream < String, Integer > tagCount = tweets.mapToPair(
   new PairFunction < Tweet, String, Integer > () {
    public Tuple2 < String, Integer > call(Tweet tw) {
     return new Tuple2 < String, Integer > (tw.hashTag, 1);
    }
   }
  );

  JavaPairDStream < String, Integer > reducedTagCount = tagCount.reduceByKeyAndWindow(
   new Function2 < Integer, Integer, Integer > () {
    public Integer call(Integer i1, Integer i2) {
     return i1 + i2;
    }
   },
   new Duration(60 * 5 * 1000),
   new Duration(10 * 1000)
  );

  JavaPairDStream < String, Tuple2 < Tweet, Integer >> combinedCntTweet = reducedTagTweetPair.join(reducedTagCount);


  JavaPairDStream < Integer, Tweet > pairByCount = combinedCntTweet.mapToPair(
   new PairFunction < Tuple2 < Tweet, Integer > , Integer, Tweet > () {
    public Tuple2 < Integer, Tweet > call(Tuple2 < Tweet, Integer > in ) {
     return in.swap();
    }
   }
  );


  pairByCount.print();
  sc.start();
  sc.awaitTermination();
 }
}
  JavaDStream < RawTweet > rawTweets = stream.map(new Function < Status, RawTweet > () {
   public RawTweet call(Status status) {
    RawTweet rt = new RawTweet(status.getUser().getScreenName(), status.getText());
    return rt;
   }
  });

   < Tweet > tweets = rawTweets.flatMap(
   new FlatMapFunction < RawTweet, Tweet > () {
    public Iterable<Tweet> call(RawTweet rt) {
     List < String > words = Arrays.asList(rt.text.split(" "));
     List < Tweet > tw = new ArrayList < Tweet > ();
     List < String > tags = new ArrayList < String > ();
     List < String > mention = new ArrayList < String > ();
     for (String word: words) {
      if (word.startsWith("#")) {
       tags.add(word);
      } else if (word.startsWith("@")) {
       mention.add(word);
      }
     }

     for (String tag: tags) {
      tw.add(new Tweet(tag, rt.user, mention));
     }
     return tw;
    }
   }
  );

  JavaPairDStream < String, Tweet > tagTweetPair = tweets.mapToPair(
   new PairFunction < Tweet, String, Tweet > () {
    public Tuple2 < String, Tweet > call(Tweet t ) {
     return new Tuple2 < String, Tweet > ( t.hashTag, t);
    }
   }
  );

  JavaPairDStream < String, Tweet > reducedTagTweetPair = tagTweetPair.reduceByKeyAndWindow(
   new Function2 < Tweet, Tweet, Tweet > () {
    public Tweet call(Tweet t1, Tweet t2) {
    String combinedAuth = t1.author+","+t2.author;
    t1.getMentions().addAll(t2.getMentions());
    Tweet result = new Tweet(t1.hashTag,combinedAuth,t1.getMentions());
    return result;
    }
   },
   new Duration(60 * 5 * 1000),
   new Duration(10 * 1000)
  );


  JavaPairDStream < String, Integer > tagCount = tweets.mapToPair(
   new PairFunction < Tweet, String, Integer > () {
    public Tuple2 < String, Integer > call(Tweet tw ) {
     return new Tuple2 < String, Integer > ( tw.hashTag, 1);
    }
   }
  );

  JavaPairDStream < String, Integer > reducedTagCount = tagCount.reduceByKeyAndWindow(
   new Function2 < Integer, Integer, Integer > () {
    public Integer call(Integer i1, Integer i2) {
     return i1 + i2;
    }
   },
   new Duration(60 * 5 * 1000),
   new Duration(10 * 1000)
  );

  JavaPairDStream <String,Tuple2<Tweet,Integer>> combinedCntTweet = reducedTagTweetPair.join(reducedTagCount); 

  JavaPairDStream<Integer,Tweet> pairByCount = combinedCntTweet.mapToPair(
    new PairFunction<String,Integer,Tweet>(){
      public Tuple2<Integer,Tweet> call(String in){
        return new Tuple2 < Integer, Tweet > ( 5, new Tweet());
        
      }
    }
  );


pairByCount.print();
  sc.start();
  sc.awaitTermination();
}
}
