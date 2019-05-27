package kafka.rest;

import kafka.model.Tweet;
import kafka.model.Twitter;
import kafka.model.User;
import kafka.utility.TweetFilter;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class SSERoutine implements Runnable {

    private final TweetStub tweetStub;
    private long timestamp;
    private final long window = Duration.ofSeconds(3).getSeconds() * 1000;

    public SSERoutine(long timestamp, TweetStub tweetStub) {
        this.tweetStub = tweetStub;
        this.timestamp = timestamp;
    }

    @Override
    public void run() {
        checkSSEForUsers();
    }

    private void checkSSEForUsers() {
        ArrayDeque<User> users = (ArrayDeque<User>) Twitter.getTwitter().getUsers();
        boolean stop = false;
        int iterations = 0;

        while (!stop && (iterations != users.size())) {
            User user = users.getFirst();
            if ((timestamp - user.getSubscriptionStub().lastPoll()) > window) {
                //get subscription
                SubscriptionStub subscription = user.getSubscriptionStub();

                List<String> locationsFollowed = subscription.getLocationsFollowed();
                List<String> tagsFollowed = subscription.getTagsFollowed();
                List<String> usersFollowed = subscription.getUsersFollowed();

                List<Tweet> partialTweetsLocations = new ArrayList<>();
                List<Tweet> partialTweetsTags = new ArrayList<>();
                List<Tweet> partialTweetsMentions = new ArrayList<>();

                //poll()

                List<Tweet> tweets = new ArrayList<>();
                if (!locationsFollowed.isEmpty()) {
                    List<Tweet> finalPartialTweetsLocations = partialTweetsLocations;
                    locationsFollowed.forEach(location -> finalPartialTweetsLocations.addAll(tweetStub.findTweetsSubscription(user.getId(),
                            Collections.singletonList(location),
                            Collections.singletonList("@all"),
                            Collections.singletonList("#all"))));
                }
                    System.out.println(tweets.size());
                partialTweetsLocations = partialTweetsLocations.stream().distinct().collect(Collectors.toList());
                if (!tagsFollowed.isEmpty()) {
                    List<Tweet> finalPartialTweetsTags = partialTweetsTags;
                    tagsFollowed.forEach(tag -> finalPartialTweetsTags.addAll(tweetStub.findTweetsSubscription(user.getId(),
                            Collections.singletonList("all"),
                            Collections.singletonList("@all"),
                            Collections.singletonList(tag))));
                    System.out.println(tweets.size());
                    partialTweetsTags = partialTweetsTags.stream().distinct().collect(Collectors.toList());
                }
                if (!usersFollowed.isEmpty()) {
                    List<Tweet> finalPartialTweetsMentions = partialTweetsMentions;
                    usersFollowed.forEach(mention -> finalPartialTweetsMentions.addAll(tweetStub.findTweetsSubscription(user.getId(),
                            Collections.singletonList("all"),
                            Collections.singletonList(mention),
                            Collections.singletonList("#all"))));
                    System.out.println(tweets.size());
                    partialTweetsMentions = partialTweetsMentions.stream().distinct().collect(Collectors.toList());
                }

                List<Tweet> partialResult = TweetFilter.sort(partialTweetsLocations, partialTweetsTags);
                tweets = TweetFilter.sort(partialResult, partialTweetsMentions);

                //filter duplicate tweets
                final List<Tweet> ts = tweets.stream().distinct().collect(Collectors.toList());

                System.out.println("Location ");
                locationsFollowed.forEach(System.out::println);
                System.out.println("tag");
                tagsFollowed.forEach(System.out::println);
                System.out.println("mention");
                usersFollowed.forEach(System.out::println);

                System.out.println("tweets ");
                ts.forEach(tweet -> System.out.println(tweet.getContent()));
                System.out.println("end tweets ");
                LoggerFactory.getLogger(TwitterRest.class).info("size" + users.size());
                user.notityTweets(ts);

                //update poll
                subscription.updatePoll(timestamp);

                //add at the end of the queue
                users.removeFirst();
                users.addLast(user);
            } else {
                stop = true;
            }
            iterations++;
        }
    }
}
