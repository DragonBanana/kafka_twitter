package kafka.rest;

import com.google.gson.Gson;
import kafka.db.AzureDBConn;
import kafka.model.*;
import kafka.utility.ConsumerFactory;
import kafka.utility.ProducerFactory;
import kafka.utility.TweetFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class TweetStub {

    /**
     * Save the tweet.
     *
     * @param tweet the tweet that has to be saved.
     * @return the tweet that has been saved.
     */

    Tweet save(Tweet tweet) {

        //Get the filters used in the tweet.
        List<String> tweetFilters = tweet.getFilters();
        List<ProducerRecord<String, String>> records = new ArrayList<>();

        //Check where the tweet must be saved
        for (String filter : tweetFilters) {
            records.add(new ProducerRecord<>(filter, tweet.getLocation(),
                    new Gson().toJson(tweet, Tweet.class)));
        }

        long timestamp = 0;

        //Send data to Kafka
        for (ProducerRecord<String, String> record : records) {
            Producer<String, String> producer = ProducerFactory.getTweetProducer();

            producer.initTransactions();
            try {
                producer.beginTransaction();
                timestamp = producer.send(record).get().timestamp();
                producer.commitTransaction();
            } catch (ProducerFencedException e) {
                producer.close();
            } catch (KafkaException e) {
                producer.abortTransaction();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            } finally {
                producer.close();
            }
        }

        if (Twitter.getTwitter().isSSEDone()) {
            System.out.println("Start SSE Routine");
            Twitter.getTwitter().startSSE(timestamp);
        } else
            System.out.println("Already executing SSE");
        return tweet;

    }

    /**
     * Main function to search tweets with the normal consumer group strategy
     * @param id               of requester.
     * @param locationToFollow location filters.
     * @param userToFollow     tag filters.
     * @param tagToFollow      mention filters.
     * @return the latest tweet filtered using the filters params.
     */
    List<Tweet> findTweets(String id, List<String> locationToFollow, List<String> userToFollow, List<String> tagToFollow) {
        return findTweets(id, locationToFollow, userToFollow, tagToFollow, false);
    }

    /**
     * Function used to retrieve tweets in the SSERoutine
     *
     * @param id               of requester.
     * @param locationToFollow location filters.
     * @param userToFollow     tag filters.
     * @param tagToFollow      mention filters.
     * @return the latest tweet filtered using the filters params.
     */
    List<Tweet> findTweetsSubscription(String id, List<String> locationToFollow, List<String> userToFollow, List<String> tagToFollow) {
        return findTweets(id, locationToFollow, userToFollow, tagToFollow, true);
    }

    /**
     * Generic function for searching tweets given the filters.
     *
     * @param id               of requester.
     * @param locationToFollow location filters.
     * @param userToFollow     tag filters.
     * @param tagToFollow      mention filters.
     * @param subscribe        used for using the singleton consumer group
     * @return the latest tweet filtered using the filters params.
     */
    private List<Tweet> findTweets(String id, List<String> locationToFollow, List<String> userToFollow, List<String> tagToFollow, boolean subscribe) {

        //taking out filters by locations, userFollowed and tags
        String locationFilters = StringUtils.join(locationToFollow, "");
        String tagFilters = StringUtils.join(tagToFollow, "");
        String mentionFilters = StringUtils.join(userToFollow, "");
        String filter = locationFilters + tagFilters + mentionFilters;

        if (locationToFollow == null ||
                locationToFollow.size() == 0 ||
                (locationToFollow.get(0).equals("all") && locationToFollow.size() == 1))
            locationToFollow = new ArrayList<>();
        if (tagToFollow == null ||
                tagToFollow.size() == 0 ||
                (tagToFollow.get(0).equals("#all") && tagToFollow.size() == 1))
            tagToFollow = new ArrayList<>();
        if (userToFollow == null ||
                userToFollow.size() == 0 ||
                (userToFollow.get(0).equals("@all") && userToFollow.size() == 1))
            userToFollow = new ArrayList<>();

        List<Tweet> tweets;
        if (!locationToFollow.isEmpty()) {
            List<Consumer<String, String>> consumerGroup;
            if (subscribe){
                consumerGroup = ConsumerFactory.getAllSubscribeConsumerGroup();
                tweets = findLatestByLocations(id, locationToFollow, filter, consumerGroup);
            }else{
                consumerGroup = ConsumerFactory.getConsumerGroup(Topic.LOCATION, id);
                tweets = findLatestByLocations(id, locationToFollow, filter, consumerGroup);
                consumerGroup.forEach(c -> c.close(Duration.ofMillis(5000)));
            }
        } else if (userToFollow.isEmpty()) {
            List<Consumer<String, String>> consumerGroup;
            if (subscribe) {
                consumerGroup = ConsumerFactory.getAllSubscribeConsumerGroup();
                tweets = findLatestByTags(id, tagToFollow, filter, consumerGroup);
            }else {
                consumerGroup = ConsumerFactory.getConsumerGroup(Topic.TAG, id);
                tweets = findLatestByTags(id, tagToFollow, filter, consumerGroup);
                consumerGroup.forEach(c -> c.close(Duration.ofMillis(5000)));
            }
        } else {
            List<Consumer<String, String>> consumerGroup;
            if (subscribe) {
                consumerGroup = ConsumerFactory.getAllSubscribeConsumerGroup();
                tweets = findLatestByMentions(id, userToFollow, filter, consumerGroup);
            }
            else {
                consumerGroup = ConsumerFactory.getConsumerGroup(Topic.MENTION, id);
                tweets = findLatestByMentions(id, userToFollow, filter, consumerGroup);
                consumerGroup.forEach(c -> c.close(Duration.ofMillis(5000)));
            }
        }
        System.out.println("Before filtering");
        tweets.forEach(System.out::println);

        tweets = TweetFilter.filterByLocations(tweets, locationToFollow);
        tweets = TweetFilter.filterByMentions(tweets, userToFollow);
        tweets = TweetFilter.filterByTags(tweets, tagToFollow);

        System.out.println("After filtering");
        tweets.forEach(System.out::println);
        return tweets;
    }

    /**
     * Return the latest tweet filtered by location.
     *
     * @param id       the identifier of the requester.
     * @param locations the locations.
     * @param filter   the filters for the research.
     * @return the latest tweet filtered by location.
     */
    List<Tweet> findLatestByLocations(String id, List<String> locations, String filter, List<Consumer<String, String>> consumerGroup) {
 /*       //The topic we are reading from.
        String topic = Topic.LOCATION;
        //Getting the consumer.
        //TODO: make consumer group id unique
        byte[] array = new byte[7]; // length is bounded by 7
        new Random().nextBytes(array);
        String generatedString = new String(array, Charset.forName("UTF-8"));
        List<Consumer<String,String>> consumers =  ConsumerFactory.getConsumerGroup(topic,generatedString);
*/
        /*
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        consumers.forEach(consumer -> {
            Future future = executorService.submit(new ConsumerThread(consumer, id, filter);
            List<Tweet> ts = new ArrayList<>();
            try {
                ts = (List<Tweet>) future.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });*/

        return consumerGroup.stream().parallel().map(consumer -> {
            //consumer.poll(Duration.ofMillis(0));
            Set<TopicPartition> partitions = consumer.assignment();
            partitions.forEach(part -> {
                long offset = new AzureDBConn().get(new OffsetKey(id, filter, part.partition())).getValue().getOffset();
                //Moving the offset.
                System.out.println("consumer: " + consumer + " partition: " + part + " offset: " + offset);
                consumer.seek(part, offset);
            });
            List<Tweet> ts = new ArrayList<>();
            List<Tweet> tweets = new ArrayList<>();

            //Polling the data.
            do {
                ts.clear();
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                //Transforming data and filtering. (!Only by location)

                records.forEach(record -> {
                    System.out.println("record: " + record.value());
                    Tweet t = new Gson().fromJson(record.value(), Tweet.class);
                    if (locations.contains(t.getLocation())){
                        System.out.println("twitter content: " + t.getContent());
                        ts.add(t);
                    }
                });
                tweets.addAll(ts);
            } while (!ts.isEmpty());

            //Getting the new offset.
            partitions.forEach(part ->{
                long offset = consumer.position(part);
                System.out.println("part: " + part + " new offset: " + offset);
                //Saving the new offset for EOS.
                new AzureDBConn().put(new Offset(id, filter, part.partition(), offset));
            });
            return tweets;
        }).reduce(TweetFilter::sort).orElseGet(null);
    }

    /**
     * Return the latest tweet filtered by locations.
     *
     * @param id        the identifier of the requester.
     * @param locations the locations.
     * @param filter    the filters for the research.
     * @return the latest tweet filtered by location.
     */
    /*List<Tweet> findLatestByLocations(String id, List<String> locations, String filter) {
        locations.forEach(l-> System.out.println("location: " + l));
        return locations.stream()
                .map(l -> findLatestByLocation(id, l, filter))
                .reduce((l1, l2) -> {
                    l1.addAll(l2);
                    return l1;
                }).orElseGet(null);
    }
    */
    /**
     * Return the latest tweet filtered by tags.
     *
     * @param id     the identifier of the requester.
     * @param tags   the list of tags.
     * @param filter the filters for the research.
     * @return the latest tweet filtered by tags.
     */
    List<Tweet> findLatestByTags(String id, List<String> tags, String filter, List<Consumer<String, String>> consumerGroup) {
      /*  //The topic we are reading from.
        String topic = Topic.TAG;
        //TODO: make consumer group id unique
        byte[] array = new byte[7]; // length is bounded by 7
        new Random().nextBytes(array);
        String generatedString = new String(array, Charset.forName("UTF-8"));
        List<Consumer<String,String>> consumers =  ConsumerFactory.getConsumerGroup(topic,generatedString);
        //List<Consumer<String, String>> consumers = ConsumerFactory.getConsumerGroup(topic,"consumer-group");
*/
        return consumerGroup.stream().parallel().map(consumer -> {
            consumer.poll(Duration.ofMillis(0));
            Set<TopicPartition> partitions = consumer.assignment();
            partitions.forEach(part -> {
                long offset = new AzureDBConn().get(new OffsetKey(id, filter, part.partition())).getValue().getOffset();
                //Moving the offset.
                System.out.println("consumer: " + consumer + " partition: " + part + " offset: " + offset);
                consumer.seek(part, offset);
            });
            List<Tweet> ts = new ArrayList<>();
            List<Tweet> tweets = new ArrayList<>();

            //Polling the data.
            do {
                ts.clear();
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                //Transforming data and filtering. (!Only by tag)

                records.forEach(record -> {
                    System.out.println("record: " + record.value());
                    Tweet t = new Gson().fromJson(record.value(), Tweet.class);
                    if (t.getTags().stream().anyMatch(tags::contains)) {
                        System.out.println("twitter content: " + t.getContent());
                        ts.add(t);
                    }
                });
                tweets.addAll(ts);
            } while (!ts.isEmpty());

            //Getting the new offset.
            partitions.forEach(part ->{
                long offset = consumer.position(part);
                //Saving the new offset for EOS.
                System.out.println("part: " + part + " new offset: " + offset);
                new AzureDBConn().put(new Offset(id, filter, part.partition(), offset));
            });
            return tweets;
        }).reduce(TweetFilter::sort).orElseGet(null);
    }

    /**
     * Return the latest tweet filtered by Mention.
     *
     * @param id       the identifier of the requester.
     * @param mentions the user mentioned filters.
     * @param filter   the filters for the research.
     * @return the latest tweet filtered by user.
     */
    List<Tweet> findLatestByMentions(String id, List<String> mentions, String filter, List<Consumer<String, String>> consumerGroup) {
/*
        //The topic we are reading from.
        String topic = Topic.MENTION;
        //TODO: make consumer group id unique
        byte[] array = new byte[7]; // length is bounded by 7
        new Random().nextBytes(array);
        String generatedString = new String(array, Charset.forName("UTF-8"));
        List<Consumer<String,String>> consumers =  ConsumerFactory.getConsumerGroup(topic,generatedString);
        //List<Consumer<String,String>> consumers =  ConsumerFactory.getConsumerGroup(topic,"consumer-group");
*/
        return consumerGroup.stream().parallel().map(consumer -> {
            consumer.poll(Duration.ofMillis(0));
            Set<TopicPartition> partitions = consumer.assignment();
            partitions.forEach(part -> {
                long offset = new AzureDBConn().get(new OffsetKey(id, filter, part.partition())).getValue().getOffset();
                //Moving the offset.
                System.out.println("consumer: " + consumer + " partition: " + part + " offset: " + offset);
                consumer.seek(part, offset);
            });
            List<Tweet> ts = new ArrayList<>();
            List<Tweet> tweets = new ArrayList<>();

            //Polling the data.
            do {
                ts.clear();
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                //Transforming data and filtering. (!Only by mention)

                records.forEach(record -> {
                    System.out.println("record: " + record.value());
                    Tweet t = new Gson().fromJson(record.value(), Tweet.class);
                    if (t.getMentions().stream().anyMatch(mentions::contains)) {
                        System.out.println("twitter content: " + t.getContent());
                        ts.add(t);
                    }
                });
                tweets.addAll(ts);
            } while (!ts.isEmpty());

            //Getting the new offset.
            partitions.forEach(part ->{
                long offset = consumer.position(part);
                //Saving the new offset for EOS.
                System.out.println("part: " + part + " new offset: " + offset);
                new AzureDBConn().put(new Offset(id, filter, part.partition(), offset));
            });

            return tweets;
        }).reduce(TweetFilter::sort).orElseGet(null);
    }

    public boolean subscription(String id, List<String> locations, List<String> tags, List<String> mentions) {

        if (locations == null ||
                locations.isEmpty() ||
                (locations.get(0).equals("all") && locations.size() == 1))
            locations = new ArrayList<>();
        if (tags == null ||
                tags.isEmpty() ||
                (tags.get(0).equals("#all") && tags.size() == 1))
            tags = new ArrayList<>();
        if (mentions == null ||
                mentions.isEmpty() ||
                (mentions.get(0).equals("@all") && mentions.size() == 1))
            mentions = new ArrayList<>();

        User user = Twitter.getTwitter().getUser(id);
        //check if WebSocket connection is open
        if (!user.getVirtualClient().isConnected()) {
            return false;
        }

        //creation of subscriptions
        SubscriptionStub subStub = user.getSubscriptionStub();
        locations.forEach(subStub::followLocation);
        tags.forEach(subStub::followTag);
        mentions.forEach(subStub::followUser);

        return true;
    }


}
