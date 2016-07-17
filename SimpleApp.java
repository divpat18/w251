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

public class SimpleApp {
 public static void main(String[] args) {
  String consumerKey = args[0];
  String consumerSecret = args[1];
  String accessToken = args[2];
  String accessTokenSecret = args[3];
  final int numberOfTweetsToPrint = Integer.valueOf(args[4]);


  System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
  System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
  System.setProperty("twitter4j.oauth.accessToken", accessToken);
  System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

  SparkConf conf = new SparkConf().setAppName("Simple Application");
  JavaStreamingContext sc = new JavaStreamingContext(new JavaSparkContext(conf), Durations.seconds(10));

  JavaReceiverInputDStream < Status > stream = TwitterUtils.createStream(sc);

  //Extract Java Object from tweets. Contains Text and User
  JavaDStream < RawTweet > rawTweets = stream.map(new Function < Status, RawTweet > () {
   public RawTweet call(Status status) {
    RawTweet rt = new RawTweet(status.getUser().getScreenName(), status.getText());
    return rt;
   }
  });

  //Break the text down into hashtags and mentions
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
  //Convert into a pair of hashTag and Tweet object
  JavaPairDStream < String, Tweet > tagTweetPair = tweets.mapToPair(
   new PairFunction < Tweet, String, Tweet > () {
    public Tuple2 < String, Tweet > call(Tweet t) {
     return new Tuple2 < String, Tweet > (t.hashTag, t);
    }
   }
  );
  //Reduce by hashTag and combine Tweet object
  JavaPairDStream < String, Tweet > reducedTagTweetPair = tagTweetPair.reduceByKeyAndWindow(
   new Function2 < Tweet, Tweet, Tweet > () {
    public Tweet call(Tweet t1, Tweet t2) {
     String combinedAuth = t1.author + "," + t2.author;
     t1.getMentions().addAll(t2.getMentions());
     Tweet result = new Tweet(t1.hashTag, combinedAuth, t1.getMentions());
     return result;
    }
   },
   new Duration(10 * 12 * 1000)
  );

  //Convert Hashtags into pairs
  JavaPairDStream < String, Integer > tagCount = tweets.mapToPair(
   new PairFunction < Tweet, String, Integer > () {
    public Tuple2 < String, Integer > call(Tweet tw) {
     return new Tuple2 < String, Integer > (tw.hashTag, 1);
    }
   }
  );
  //Count Instances of Hashtags
  JavaPairDStream < String, Integer > reducedTagCount = tagCount.reduceByKeyAndWindow(
   new Function2 < Integer, Integer, Integer > () {
    public Integer call(Integer i1, Integer i2) {
     return i1 + i2;
    }
   },
   new Duration(10 * 12 * 1000)

  );
  //  Combine the count and Tweet Object
  JavaPairDStream < String, Tuple2 < Tweet, Integer >> combinedCntTweet = reducedTagTweetPair.join(reducedTagCount);

  JavaPairDStream < Integer, Tweet > pairByCount = combinedCntTweet.mapToPair(
   new PairFunction < Tuple2 < String, Tuple2 < Tweet, Integer >> , Integer, Tweet > () {
    public Tuple2 < Integer, Tweet > call(Tuple2 < String, Tuple2 < Tweet, Integer >> in ) {
     return in._2.swap();
    }
   }
  );
  //Sort by Counts
  JavaPairDStream < Integer, Tweet > sortedCounts = pairByCount.transformToPair(
   new Function < JavaPairRDD < Integer, Tweet > , JavaPairRDD < Integer, Tweet >> () {
    public JavaPairRDD < Integer, Tweet > call(JavaPairRDD < Integer, Tweet > in ) throws Exception {
     return in.sortByKey(false);
    }
   });

  //Print top n tweets
  sortedCounts.foreach(
   new Function < JavaPairRDD < Integer, Tweet > , Void > () {
    public Void call(JavaPairRDD < Integer, Tweet > rdd) {
     String out = "\nTop " + numberOfTweetsToPrint + " hashtags at 2 min intervals:\n";
     for (Tuple2 < Integer, Tweet > t: rdd.take(numberOfTweetsToPrint)) {
      out = out + t.toString() + "\n";
     }
     System.out.println(out);
     return null;
    }
   }
  );

  //Repeat for 30 min interval
  JavaPairDStream < String, Tweet > reducedTagTweetPair30 = tagTweetPair.reduceByKeyAndWindow(
   new Function2 < Tweet, Tweet, Tweet > () {
    public Tweet call(Tweet t1, Tweet t2) {
     String combinedAuth = t1.author + "," + t2.author;
     t1.getMentions().addAll(t2.getMentions());
     Tweet result = new Tweet(t1.hashTag, combinedAuth, t1.getMentions());
     return result;
    }
   },
   new Duration(60 * 30 * 1000)
  );


  JavaPairDStream < String, Integer > reducedTagCount30 = tagCount.reduceByKeyAndWindow(
   new Function2 < Integer, Integer, Integer > () {
    public Integer call(Integer i1, Integer i2) {
     return i1 + i2;
    }
   },
   new Duration(60 * 30 * 1000)

  );

  JavaPairDStream < String, Tuple2 < Tweet, Integer >> combinedCntTweet30 = reducedTagTweetPair30.join(reducedTagCount);

  JavaPairDStream < Integer, Tweet > pairByCount30 = combinedCntTweet30.mapToPair(
   new PairFunction < Tuple2 < String, Tuple2 < Tweet, Integer >> , Integer, Tweet > () {
    public Tuple2 < Integer, Tweet > call(Tuple2 < String, Tuple2 < Tweet, Integer >> in ) {
     return in._2.swap();
    }
   }
  );

  JavaPairDStream < Integer, Tweet > sortedCounts30 = pairByCount30.transformToPair(
   new Function < JavaPairRDD < Integer, Tweet > , JavaPairRDD < Integer, Tweet >> () {
    public JavaPairRDD < Integer, Tweet > call(JavaPairRDD < Integer, Tweet > in ) throws Exception {
     return in.sortByKey(false);
    }
   });


  sortedCounts.foreach(
   new Function < JavaPairRDD < Integer, Tweet > , Void > () {
    public Void call(JavaPairRDD < Integer, Tweet > rdd) {
     String out = "\nTop " + numberOfTweetsToPrint + " hashtags at 30 min intervals:\n";
     for (Tuple2 < Integer, Tweet > t: rdd.take(numberOfTweetsToPrint)) {
      out = out + t.toString() + "\n";
     }
     System.out.println(out);
     return null;
    }
   }
  );

  sc.start();
  sc.awaitTermination(30 * 60 * 1000);
 }

}
