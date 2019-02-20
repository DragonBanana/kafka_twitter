package kafka.model;

import java.util.List;

public class Tweet {

    /**
     * The author of the tweet.
     */
    private String Author;

    /**
     * The content of the tweet.
     */
    private String content;

    /**
     * The timestamp of the tweet
     */
    private String timestamp;

    /**
     * The location of the tweet.
     */
    private String location;

    /**
     * The tags of the tweet.
     */
    private List<String> tags;

    /**
     * The mentions of the tweet.
     */
    private List<String> mentions;

    /**
     * Constructor
     * @param author the author of the tweet.
     * @param content the content of the tweet.
     * @param timestamp the timestamp of the tweet.
     * @param location the location of the tweet.
     * @param tags the tags of the tweet.
     * @param mentions the mentions of the tweet.
     */
    public Tweet(String author, String content, String timestamp, String location, List<String> tags, List<String> mentions) {
        Author = author;
        this.content = content;
        this.timestamp = timestamp;
        this.location = location;
        this.tags = tags;
        this.mentions = mentions;
    }

    /**
     * Return the author of the tweet.
     * @return the author of the tweet.
     */
    public String getAuthor() {
        return Author;
    }

    /**
     * Return the content of the tweet.
     * @return the content of the tweet.
     */
    public String getContent() {
        return content;
    }

    /**
     * Return the timestamp of the tweet.
     * @return the timestamp of the tweet.
     */
    public String getTimestamp() {
        return timestamp;
    }

    /**
     * Return the location of the tweet.
     * @return the location of the tweet.
     */
    public String getLocation() {
        return location;
    }

    /**
     * Return the tags of the tweet.
     * @return the tags of the tweet.
     */
    public List<String> getTags() {
        return tags;
    }

    /**
     * Return the mentions of the tweet.
     * @return the mentions of the tweet.
     */
    public List<String> getMentions() {
        return mentions;
    }
}
