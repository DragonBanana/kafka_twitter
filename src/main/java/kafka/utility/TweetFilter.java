package kafka.utility;

import kafka.model.Tweet;

import java.util.ArrayList;
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
        if(locations.isEmpty()) {
            return tweets;
        }
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

    public static List<Tweet> sort(List<Tweet> list1, List<Tweet> list2) {
        List<Tweet> t1;
        List<Tweet> t2;
        List<Tweet> result = new ArrayList<>();

        //Check that the lists are not empty
        if (list1.isEmpty() && list2.isEmpty())
            return result;
        if (list1.isEmpty()) {
            result.addAll(list2);
            return result;
        }
        if (list2.isEmpty()) {
            result.addAll(list1);
            return result;
        }

        if (list1.size() > list2.size()) {
            t1 = list1;
            t2 = list2;
        } else {
            t2 = list1;
            t1 = list2;
        }

        long upperBound;
        long currentValue = Long.parseLong(t2.get(0).getTimestamp());

        for (Tweet tweet : t1) {
            upperBound = Long.parseLong(tweet.getTimestamp());

            if (!t2.isEmpty() && upperBound > currentValue) {
                //if upperbound is greater than current value add the element of the second list to the result
                result.add(t2.get(0));
                t2.remove(0);

                if (!t2.isEmpty())
                    currentValue = Long.parseLong(t2.get(0).getTimestamp());
            }

            //add the element of the first list anyway
            result.add(tweet);
        }

        //if there are some elements greater than the last element of t1 addAll in the tail
        if (!t2.isEmpty()) {
            result.addAll(t2);
            t2.clear();
        }

        return result;
    }
}
