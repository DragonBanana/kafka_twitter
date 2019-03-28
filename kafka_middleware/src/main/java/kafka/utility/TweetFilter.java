package kafka.utility;

import kafka.model.Tweet;

import java.util.List;
import java.util.stream.Collectors;

public class TweetFilter {

    public static List<Tweet> filterByLocations(List<Tweet> tweets, List<String> locations) {
        return tweets.stream()
                .filter(t -> locations.stream()
                        .anyMatch(loc -> t.getLocation().equals(loc)))
                .collect(Collectors.toList());
    }

    public static List<Tweet> filterByTags(List<Tweet> tweets, List<String> tags) {
        return tweets.stream()
                .filter(t -> tags.stream()
                        .anyMatch(tag -> t.getTags().stream()
                                .anyMatch(ttag -> ttag.equals(tag))))
                .collect(Collectors.toList());
    }

    public static List<Tweet> filterByMentions(List<Tweet> tweets, List<String> mentions) {
        return tweets.stream()
                .filter(t -> mentions.stream()
                        .anyMatch(mention -> t.getMentions().stream()
                                .anyMatch(tmention -> tmention.equals(mention))))
                .collect(Collectors.toList());
    }

}
