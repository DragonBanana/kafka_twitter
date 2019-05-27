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
                    partialTweetsLocations = locationsFollowed
                            .stream()
                            .map(location -> tweetStub.findTweetsSubscription(user.getId(),
                                    Collections.singletonList(location),
                                    Collections.singletonList("@all"),
                                    Collections.singletonList("#all")))
                            .reduce((l1,l2) -> {l1.addAll(l2); return l1;})
                            .get();
                    partialTweetsLocations = partialTweetsLocations
                            .parallelStream()
                            .distinct()
                            .sorted((x, y) -> {
                                if(Long.parseLong(x.getTimestamp()) > Long.parseLong(y.getTimestamp()))
                                    return 1;
                                else if(Long.parseLong(x.getTimestamp()) == Long.parseLong(y.getTimestamp()))
                                    return 0;
                                return -1;
                            })
                            .collect(Collectors.toList());
                }
                if (!tagsFollowed.isEmpty()) {
                    partialTweetsTags = tagsFollowed
                            .stream()
                            .map(tag -> tweetStub.findTweetsSubscription(user.getId(),
                                    Collections.singletonList("all"),
                                    Collections.singletonList("@all"),
                                    Collections.singletonList(tag)))
                            .reduce((l1,l2) -> {l1.addAll(l2); return l1;})
                            .get();
                    System.out.println(partialTweetsTags.size());
                    partialTweetsTags = partialTweetsTags
                            .parallelStream()
                            .distinct()
                            .sorted((x, y) -> {
                                if(Long.parseLong(x.getTimestamp()) > Long.parseLong(y.getTimestamp()))
                                    return 1;
                                else if(Long.parseLong(x.getTimestamp()) == Long.parseLong(y.getTimestamp()))
                                    return 0;
                                return -1;
                            })
                            .collect(Collectors.toList());

                }
                if (!usersFollowed.isEmpty()) {
                    partialTweetsMentions = usersFollowed
                            .stream()
                            .map(mention -> tweetStub.findTweetsSubscription(user.getId(),
                                    Collections.singletonList("all"),
                                    Collections.singletonList(mention),
                                    Collections.singletonList("#all")))
                            .reduce((l1,l2) -> {l1.addAll(l2); return l1;})
                            .get();
                    partialTweetsMentions = partialTweetsMentions
                            .parallelStream()
                            .distinct()
                            .sorted((x, y) -> {
                                if(Long.parseLong(x.getTimestamp()) > Long.parseLong(y.getTimestamp()))
                                    return 1;
                                else if(Long.parseLong(x.getTimestamp()) == Long.parseLong(y.getTimestamp()))
                                    return 0;
                                return -1;
                            })
                            .collect(Collectors.toList());
                }

                List<Tweet> partialResult = TweetFilter.sort(partialTweetsLocations, partialTweetsTags);
                partialResult.stream().reduce((l1,l2) -> {
                    if(Long.parseLong(l1.getTimestamp()) > Long.parseLong(l2.getTimestamp())) {
                        System.out.println("Urca, alla fine non li hai riordinati bene");
                        System.out.println(l1.getTimestamp() + " is > then " + l2.getTimestamp());
                    }
                    return l2;
                });
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
