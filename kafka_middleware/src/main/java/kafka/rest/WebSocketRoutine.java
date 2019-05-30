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

public class WebSocketRoutine implements Runnable {

    private final TweetStub tweetStub;
    private long timestamp;
    private final long window = Duration.ofSeconds(3).getSeconds() * 1000;

    public WebSocketRoutine(long timestamp, TweetStub tweetStub) {
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

                final List<Tweet> tweets = new ArrayList<>();
                if (!locationsFollowed.isEmpty())
                    locationsFollowed.forEach(location -> tweets.addAll(tweetStub.findTweets(user.getId(),
                            Collections.singletonList(location),
                            Collections.singletonList("@all"),
                            Collections.singletonList("#all"))));
                    System.out.println(tweets.size());
                if (!tagsFollowed.isEmpty()) {
                    tagsFollowed.forEach(tag -> tweets.addAll(tweetStub.findTweets(user.getId(),
                            Collections.singletonList("all"),
                            Collections.singletonList("@all"),
                            Collections.singletonList(tag))));
                    System.out.println(tweets.size());
                }
                if (!usersFollowed.isEmpty()) {
                    usersFollowed.forEach(mention -> tweets.addAll(tweetStub.findTweets(user.getId(),
                            Collections.singletonList("all"),
                            Collections.singletonList(mention),
                            Collections.singletonList("#all"))));
                    System.out.println(tweets.size());
                }

                //Sorting and filtering
                final List<Tweet> ts = tweets.parallelStream().sorted((l1,l2) -> {
                    if(Long.parseLong(l1.getTimestamp()) > Long.parseLong(l2.getTimestamp()))
                        return 1;
                    else if(Long.parseLong(l1.getTimestamp()) == Long.parseLong(l2.getTimestamp()))
                        return 0;
                    return -1;
                }).distinct().collect(Collectors.toList());

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
