package kafka.rest;

import kafka.model.Tweet;
import kafka.model.Twitter;
import kafka.model.User;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
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
                //todo get subscription
                SubscriptionStub subscription = user.getSubscriptionStub();

                List<String> locationsFollowed = subscription.getLocationsFollowed();
                List<String> tagsFollowed = subscription.getTagsFollowed();
                List<String> usersFollowed = subscription.getUsersFollowed();

                //TODO poll()
                //List<Tweet> tweets = tweetStub.findTweets(user.getId(), locationsFollowed, tagsFollowed, usersFollowed);

                List<Tweet> tweets = new ArrayList<>();
                LoggerFactory.getLogger(TwitterRest.class).info(locationsFollowed.toString());
                tweets.addAll(tweetStub.findTweets(user.getId(), locationsFollowed, Arrays.asList("all"), Arrays.asList("all")));
                LoggerFactory.getLogger(TwitterRest.class).info("size" + users.size());
                tweets.addAll(tweetStub.findTweets(user.getId(), Arrays.asList("all"), tagsFollowed, Arrays.asList("all")));
                LoggerFactory.getLogger(TwitterRest.class).info("size" + users.size());
                tweets.addAll(tweetStub.findTweets(user.getId(), Arrays.asList("all"), Arrays.asList("all"), usersFollowed));
                LoggerFactory.getLogger(TwitterRest.class).info("size" + users.size());

                //filter duplicate tweets
                tweets = tweets.stream().distinct().collect(Collectors.toList());

                LoggerFactory.getLogger(TwitterRest.class).info("size" + users.size());
                //todo VirtualClient.notify();
                user.notityTweets(tweets);

                //update poll
                subscription.updatePoll(timestamp);

                //add at the end of the queue
                users.removeFirst();
                users.addLast(user);
            } else {
                users.addFirst(user);
                stop = true;
            }
            iterations++;
        }
        LoggerFactory.getLogger(TwitterRest.class).info("size" + users.size());
        System.out.println("users size " + users.size());
    }
}
