/* SimpleApp.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Duration;
import twitter4j.*;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;

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

    //String logFile = "YOUR_SPARK_HOME/README.md"; // Should be some file on your system
    SparkConf conf = new SparkConf().setAppName("Simple Application");
    JavaStreamingContext sc = new JavaStreamingContext(new JavaSparkContext(conf),new Duration(60000));
    JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(sc);

    JavaDStream<String> statuses = stream.map(new Function<Status,String>(){
        public String call(Status status){return status.getText();}
        });
    //statuses.print();
    System.out.println("*****************"+statuses.count());
    statuses.print();
    //JavaRDD<String> logData = sc.textFile(logFile).cache();

    //long numAs = logData.filter(new Function<String, Boolean>() {
    //  public Boolean call(String s) { return s.contains("a"); }
    //}).count();

    //long numBs = logData.filter(new Function<String, Boolean>() {
    //  public Boolean call(String s) { return s.contains("b"); }
    //}).count();

    //System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    sc.start();
  }
}
~    
