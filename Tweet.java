public class Tweet {
 public String hashTag;
 public String author;
 public List < String > mentions;

 public Tweet() {}

 public Tweet(String ht, String auth, List < String > mnt) {
  this.hashTag = ht;
  this.authors = author;
  this.mentions = mnt;

 }

 public void setHashTag(String ht) {
  this.hashTag = ht;
 }


 public void setAuthor(String auth) {
  this.authors.add(auth);
 }

 public List < String > getMentions() {
  return mentions;
 }







}
