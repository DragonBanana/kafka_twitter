package kafka.rest;

import kafka.model.Tweet;
import kafka.model.Twitter;
import kafka.model.User;

import java.time.Duration;
import java.util.*;

public class SSERoutine implements Runnable {

    private long timestamp;
    private final long window = Duration.ofMinutes(5).getSeconds() * 1000;

    public SSERoutine(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public void run() {
        ArrayDeque<User> users = (ArrayDeque<User>) Twitter.getTwitter().getUsers();
        boolean stop = false;

        while (!stop) {
            User user = users.removeFirst();

            if ((timestamp - user.getSubscriptionStub().lastPoll()) > window) {
                //todo get subscription
                SubscriptionStub subscription = user.getSubscriptionStub();

                List<String> locationsFollowed = subscription.getLocationsFollowed();
                List<String> tagsFollowed = subscription.getTagsFollowed();
                List<String> usersFollowed = subscription.getUsersFollowed();

                //TODO poll()
                List<Tweet> tweets = new TweetStub().findTweets(user.getId(), locationsFollowed, tagsFollowed, usersFollowed);

                //todo VirtualClient.notify();
                user.getVirtualClient().notityTweets(tweets);

                //update poll
                subscription.updatePoll(timestamp);

                //add at the end of the queue
                users.addLast(user);
            } else {
                users.addFirst(user);
                stop = true;
            }
        }
    }
}
