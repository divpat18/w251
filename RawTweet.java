import java.util.*;
import java.io.*;

/*
	Object to Store Raw Tweet info
*/
public class RawTweet implements Serializable {
 public String user;
 public String text;

 public RawTweet(String user, String text) {
  this.user = user;
  this.text = text;
 }

 @Override
 public String toString() {
  return text + "||" + "Authors:" + user;
 }
}
