package kafka.rest;

import kafka.model.Tweet;
import kafka.model.Twitter;
import kafka.model.User;
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

                //poll()

                List<Tweet> tweets = new ArrayList<>();
                if (!locationsFollowed.isEmpty())
                    tweets.addAll(tweetStub.findTweets(user.getId(), locationsFollowed, Collections.singletonList("@all"), Collections.singletonList("#all")));
                if (!tagsFollowed.isEmpty()) {
                    tweets.addAll(tweetStub.findTweets(user.getId(), Collections.singletonList("all"), Collections.singletonList("@all"), tagsFollowed));
                }
                if (!usersFollowed.isEmpty()) {
                    tweets.addAll(tweetStub.findTweets(user.getId(), Collections.singletonList("all"), usersFollowed, Collections.singletonList("#all")));
                }

                //filter duplicate tweets
                tweets = tweets.stream().distinct().collect(Collectors.toList());

                LoggerFactory.getLogger(TwitterRest.class).info("size" + users.size());
                user.notityTweets(tweets);

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
