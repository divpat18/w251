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
public class SimpleApp {
  public static void main(String[] args) {
    String consumerKey = args[0];
    String consumerSecret =args[1];
    String accessToken = args[2];
    String accessTokenSecret = args[3];


    System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
    System.setProperty("twitter4j.oauth.accessToken", accessToken);
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

    SparkConf conf = new SparkConf().setAppName("Simple Application");
    JavaStreamingContext sc = new JavaStreamingContext(new JavaSparkContext(conf),Durations.seconds(10));
    JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(sc);
    
    JavaDStream<String> statuses = stream.map(new Function<Status,String>(){
	public String call(Status status){return status.getText();}
	});
    //statuses.print();
    
    JavaDStream<String> words = statuses.flatMap(
 	new FlatMapFunction<String,String>(){
		public Iterable<String> call(String in) {
         		return Arrays.asList(in.split(" "));
       		}
	}    
    );

    
	JavaDStream<Tweet> hashTags = words.filter(
     		new Function<String, Boolean>() {
       			public Boolean call(String word) { return word.startsWith("#"); }
     		}
   	).map(new Function<String,Tweet>(){
	public Tweet call(String s)
		{
			Tweet tw = new Tweet();
			tw.createTweet(s);
			return tw;
		}
	}
	);

	
	JavaPairDStream<String, Integer> tuples = hashTags.mapToPair(
      		new PairFunction<Tweet, String, Integer>() {
        		public Tuple2<String, Integer> call(Tweet in) {
          			return new Tuple2<String, Integer>(in.hashTag, 1);
        		}
      		}
    	);
    	JavaPairDStream<String, Integer> counts = tuples.reduceByKeyAndWindow(
      		new Function2<Integer, Integer, Integer>() {
        		public Integer call(Integer i1, Integer i2) { return i1 + i2; }
      		},
      		new Duration(60 * 5 * 1000),
      		new Duration(10 * 1000)
    	);
//counts.print();

JavaPairDStream<Integer, String> swappedCounts = counts.mapToPair(
     new PairFunction<Tuple2<String, Integer>, Integer, String>() {
       public Tuple2<Integer, String> call(Tuple2<String, Integer> in) {
         return in.swap();
       }
     }
   );

   JavaPairDStream<Integer, String> sortedCounts = swappedCounts.transformToPair(
     new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>() {
       public JavaPairRDD<Integer, String> call(JavaPairRDD<Integer, String> in) throws Exception {
         return in.sortByKey(false);
       }
     });
sortedCounts.print();
/*
   sortedCounts.foreach(
     new Function<JavaPairRDD<Integer, String>, Void> () {
       public Void call(JavaPairRDD<Integer, String> rdd) {
         String out = "\nTop 10 hashtags:\n";
         for (Tuple2<Integer, String> t: rdd.take(10)) {
           out = out + t.toString() + "\n";
         }
         System.out.println(out);
         return null;
       }
     }
   );*/
    sc.start();  
    sc.awaitTermination();
   }

}
