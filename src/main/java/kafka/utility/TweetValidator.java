package kafka.utility;

import kafka.model.Tweet;

import java.util.ArrayList;

public class TweetValidator {

    public static boolean isValid(Tweet tweet) {
        if(tweet.getAuthor() != null &&
        tweet.getMentions() != null &&
        tweet.getTags() != null &&
        tweet.getLocation() != null &&
        tweet.getContent() != null &&
        tweet.getTimestamp() != null &&
        tweet.getAuthor().length() > 0 &&
        tweet.getTimestamp().length() > 0 &&
        tweet.getContent().length() > 0 &&
        tweet.getLocation().length() > 0)
            return true;
        return false;
    }

    public static Tweet toStandardTweet(Tweet tweet) {
        if(tweet.getTags() == null) {
            tweet = new Tweet(tweet.getAuthor(),
                    tweet.getContent(),
                    tweet.getTimestamp(),
                    tweet.getLocation(),
                    new ArrayList<>(),
                    tweet.getMentions());
        }
        if(tweet.getMentions() == null) {
            tweet = new Tweet(tweet.getAuthor(),
                    tweet.getContent(),
                    tweet.getTimestamp(),
                    tweet.getLocation(),
                    tweet.getTags(),
                    new ArrayList<>());
        }
        return tweet;
    }

}
