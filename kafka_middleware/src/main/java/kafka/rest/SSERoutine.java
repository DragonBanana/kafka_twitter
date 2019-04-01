package kafka.rest;

import kafka.model.Tweet;
import kafka.model.Twitter;
import kafka.model.User;

import java.time.Duration;
import java.util.*;

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
            User user = users.removeFirst();

            if ((timestamp - user.getSubscriptionStub().lastPoll()) > window) {
                System.out.println("updating for " + user.getId());
                //todo get subscription
                SubscriptionStub subscription = user.getSubscriptionStub();

                List<String> locationsFollowed = subscription.getLocationsFollowed();
                List<String> tagsFollowed = subscription.getTagsFollowed();
                List<String> usersFollowed = subscription.getUsersFollowed();

                //TODO poll()
                List<Tweet> tweets = tweetStub.findTweets(user.getId(), locationsFollowed, tagsFollowed, usersFollowed);

                //todo VirtualClient.notify();
                user.notityTweets(tweets);

                //update poll
                subscription.updatePoll(timestamp);

                //add at the end of the queue
                users.addLast(user);
            } else {
                users.addFirst(user);
                stop = true;
            }
            iterations++;
        }
        System.out.println("users size " + users.size());
    }
}
