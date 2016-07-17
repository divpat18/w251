import java.util.*;
import java.io.*;

public class Tweet implements Serializable {
 public String hashTag;
 public String author;
 public List < String > mentions;

 public Tweet() {}

 public Tweet(String ht, String auth, List < String > mnt) {
  this.hashTag = ht;
  this.author = author;
  this.mentions = mnt;

 }

 public void setHashTag(String ht) {
  this.hashTag = ht;
 }


 public void setAuthor(String auth) {
  this.author = auth;
 }

 public List < String > getMentions() {
  return mentions;
 }

}
