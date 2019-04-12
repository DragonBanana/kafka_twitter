package kafka.model;

import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.List;

public class Tweet {

    /**
     * The author of the tweet.
     */
    private String author;

    /**
     * The content of the tweet.
     */
    private String content;

    /**
     * The timestamp of the tweet
     */
    private String timestamp;

    /**
     * The location of the tweet and it is supposed to be unique in a tweet
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
        this.author = author;
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
        return author;
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

    /**
     * Return the filters type used in the tweet.
     * All possible values are : location, tag, mention.
     * @return The list containig the filter type
     */
    public List<String> getFilters() {
        List<String> result = new ArrayList<>();

        System.out.println("getFilters" + new Gson().toJson(this));

        if (mentions.size() > 0)
            result.add(Topic.MENTION);

        if (tags.size() > 0)
            result.add(Topic.TAG);

        result.add(Topic.LOCATION);

        return result;
    }

    public boolean equals(Tweet t) {
        return t.getTags().containsAll(this.getTags()) &&
                this.getTags().containsAll(t.getTags()) &&
                t.getMentions().containsAll(this.getMentions()) &&
                this.getMentions().containsAll(t.getMentions()) &&
                t.getLocation().equals(this.getLocation()) &&
                t.getAuthor().equals(this.getAuthor()) &&
                t.getContent().equals(this.getContent()) &&
                t.getTimestamp().equals(this.getTimestamp());
    }

    public int hashCode() {
        String tweet = new Gson().toJson(this);
        return tweet.hashCode();
    }
}