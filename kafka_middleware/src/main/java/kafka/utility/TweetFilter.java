package kafka.utility;

import kafka.model.Tweet;

import java.util.List;
import java.util.stream.Collectors;

public class TweetFilter {

    /**
     * Filters a list of tweets by their locations.
     * @param tweets the list of tweets to be filtered.
     * @param locations the locations.
     * @return the list of tweets filtered my locations.
     */
    public static List<Tweet> filterByLocations(List<Tweet> tweets, List<String> locations) {
        return tweets.stream()
                .filter(t -> locations.stream()
                        .anyMatch(loc -> t.getLocation().equals(loc)))
                .collect(Collectors.toList());
    }

    /**
     * Filters a list of tweets by their tags.
     * @param tweets the list of tweets to be filtered.
     * @param tags the tags.
     * @return the list of tweets filtered my tags.
     */
    public static List<Tweet> filterByTags(List<Tweet> tweets, List<String> tags) {
        if(!tags.isEmpty())
            return tweets.stream()
                    .filter(t -> tags.stream()
                            .anyMatch(tag -> t.getTags().stream()
                                    .anyMatch(ttag -> ttag.equals(tag))))
                    .collect(Collectors.toList());
        return tweets;
    }

    /**
     * Filters a list of tweets by their mentions.
     * @param tweets the list of tweets to be filtered.
     * @param mentions the mentions.
     * @return the list of tweets filtered my mentions.
     */
    public static List<Tweet> filterByMentions(List<Tweet> tweets, List<String> mentions) {
        if(!mentions.isEmpty())
            return tweets.stream()
                    .filter(t -> mentions.stream()
                            .anyMatch(mention -> t.getMentions().stream()
                                    .anyMatch(tmention -> tmention.equals(mention))))
                    .collect(Collectors.toList());
        return tweets;
    }

}
