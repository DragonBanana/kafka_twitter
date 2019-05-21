package kafka.rest;

import com.google.gson.Gson;
import kafka.db.AzureDBConn;
import kafka.model.*;
import kafka.utility.ConsumerFactory;
import kafka.utility.ProducerFactory;
import kafka.utility.TopicPartitionFactory;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

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
     * Main function for searching tweets given the filters.
     *
     * @param id               of requester.
     * @param locationToFollow location filters.
     * @param userToFollow     tag filters.
     * @param tagToFollow      mention filters.
     * @return the latest tweet filtered using the filters params.
     */
    List<Tweet> findTweets(String id, List<String> locationToFollow, List<String> userToFollow, List<String> tagToFollow) {

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
            tweets = findLatestByLocations(id, locationToFollow, filter);
        } else if (userToFollow.isEmpty()) {
            //filter tweet using only tag.
            tweets = findLatestByTags(id, tagToFollow, filter);
        } else
            //filter tweet using users mentioned (and tag if present).
            tweets = findLatestByMentions(id, userToFollow, filter);


        tweets = TweetFilter.filterByLocations(tweets, locationToFollow);
        tweets = TweetFilter.filterByMentions(tweets, userToFollow);
        tweets = TweetFilter.filterByTags(tweets, tagToFollow);

        return tweets;
    }

    /**
     * Return the latest tweet filtered by location.
     *
     * @param id       the identifier of the requester.
     * @param location the location.
     * @param filter   the filters for the research.
     * @return the latest tweet filtered by location.
     */
    private List<Tweet> findLatestByLocation(String id, String location, String filter) {
        //The topic we are reading from.
        String topic = Topic.LOCATION;
        //Getting the consumer.
        Consumer<String, String> consumer = ConsumerFactory.getConsumer();
        //Getting the partition of the topic.
        int partition = TopicPartitionFactory.getLocationPartition(location);
        //Creating the topic partition object (it is required in the next instructions).
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        //Subscribe to a topic.
        consumer.assign(Collections.singletonList(topicPartition));
        //Getting the offset from the db.
        long offset = new AzureDBConn().get(new OffsetKey(id, filter, partition)).getValue().getOffset();
        //Moving the offset.
        consumer.seek(topicPartition, offset);
        consumer.poll(Duration.ofMillis(0));
        List<Tweet> ts = new ArrayList<>();
        List<Tweet> tweets = new ArrayList<>();
        //Polling the data.
        do {
            ts.clear();
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            //Transforming data and filtering. (!Only by location)
            ts = records.records(topicPartition).stream().map(record -> new Gson().fromJson(record.value(), Tweet.class)).filter(t -> t.getLocation().equals(location)).collect(Collectors.toList());
            tweets.addAll(ts);
        } while (!ts.isEmpty());

        //Getting the new offset.
        offset = consumer.position(topicPartition);
        //Saving the new offset for EOS.
        new AzureDBConn().put(new Offset(id, filter, partition, offset));
        //Returning the data.
        return tweets;
    }

    /**
     * Return the latest tweet filtered by locations.
     *
     * @param id        the identifier of the requester.
     * @param locations the locations.
     * @param filter    the filters for the research.
     * @return the latest tweet filtered by location.
     */
    List<Tweet> findLatestByLocations(String id, List<String> locations, String filter) {
        return locations.stream()
                .map(l -> findLatestByLocation(id, l, filter))
                .reduce((l1, l2) -> {
                    l1.addAll(l2);
                    return l1;
                }).orElseGet(null);
    }

    /**
     * Return the latest tweet filtered by tags.
     *
     * @param id     the identifier of the requester.
     * @param tags   the list of tags.
     * @param filter the filters for the research.
     * @return the latest tweet filtered by tags.
     */
    List<Tweet> findLatestByTags(String id, List<String> tags, String filter) {
        //The topic we are reading from.
        String topic = Topic.TAG;
        //Getting the consumer.
        Consumer<String, String> consumer = ConsumerFactory.getConsumer();
        //Declaring two partitions: the first for 1 tag search, the second for blob search.
        List<TopicPartition> topicPartitions = new ArrayList<>();
        if (tags.size() == 1) {
            //Getting the partition of the topic.
            int partition = TopicPartitionFactory.getTagPartition(tags.get(0));
            //Creating the topic partition object (it is required in the next instructions).
            topicPartitions.add(new TopicPartition(topic, partition));
        }
        //Getting the partition of the topic.
        int partition = TopicPartitionFactory.TAG_PARTITION_BLOB;
        //Creating the topic partition object (it is required in the next instructions).
        topicPartitions.add(new TopicPartition(topic, partition));
        //Subscribe to a topic.
        consumer.assign(topicPartitions);
        topicPartitions
                .forEach(topicPartition -> {
                    //Getting the offset from the db.
                    long offset = Math.max(new AzureDBConn().get(new OffsetKey(id, filter, topicPartition.partition())).getValue().getOffset(), 0);
                    //Moving the offset.
                    consumer.seek(topicPartition, offset);

                });
        List<Tweet> ts = new ArrayList<>();
        List<Tweet> tweets = new ArrayList<>();
        do {
            ts.clear();
            //Polling the data.
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            //Transforming data and filtering. (!Only by tag)
            records.forEach(record -> {
                Tweet t = new Gson().fromJson(record.value(), Tweet.class);
                if (t.getTags().stream().anyMatch(tags::contains))
                    ts.add(t);
            });
            tweets.addAll(ts);
        } while (!ts.isEmpty());
        topicPartitions.forEach(topicPartition -> {
            //Getting the new offset.
            long offset = consumer.position(topicPartition);
            //Saving the new offset for EOS.
            new AzureDBConn().put(new Offset(id, filter, topicPartition.partition(), offset));

        });
        //Returning the data
        return tweets;
    }

    /**
     * Return the latest tweet filtered by Mention.
     *
     * @param id       the identifier of the requester.
     * @param mentions the user mentioned filters.
     * @param filter   the filters for the research.
     * @return the latest tweet filtered by user.
     */
    List<Tweet> findLatestByMentions(String id, List<String> mentions, String filter) {

        //The topic we are reading from.
        String topic = Topic.MENTION;
        //Getting the consumer.
        Consumer<String, String> consumer = ConsumerFactory.getConsumer();
        //Declaring two partitions: the first for 1 mention search, the second for blob search.
        List<TopicPartition> topicPartitions = new ArrayList<>();
        if (mentions.size() == 1) {
            //Getting the partition of the topic.
            int partition = TopicPartitionFactory.getMentionPartition(mentions.get(0));
            //Creating the topic partition object (it is required in the next instructions).
            topicPartitions.add(new TopicPartition(topic, partition));
        }
        //Getting the partition of the topic.
        int partition = TopicPartitionFactory.MENTION_PARTITION_BLOB;
        //Creating the topic partition object (it is required in the next instructions).
        topicPartitions.add(new TopicPartition(topic, partition));
        //Subscribe to a topic.
        consumer.assign(topicPartitions);
        topicPartitions
                .forEach(topicPartition -> {
                    //Getting the offset from the db.
                    long offset = new AzureDBConn().get(new OffsetKey(id, filter, topicPartition.partition())).getValue().getOffset();
                    //Moving the offset.
                    consumer.seek(topicPartition, offset);
                });
        List<Tweet> tweets = new ArrayList<>();
        List<Tweet> ts = new ArrayList<>();
        do {
            ts.clear();
            //Polling the data.
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            //Transforming data and filtering. (!Only by tag)
            records.forEach(record -> {
                Tweet t = new Gson().fromJson(record.value(), Tweet.class);
                if (t.getMentions().stream().anyMatch(mentions::contains))
                    ts.add(t);
            });
            tweets.addAll(ts);
        } while (!ts.isEmpty());
        topicPartitions.forEach(topicPartition -> {
            //Getting the new offset.
            long offset = consumer.position(topicPartition);
            //Saving the new offset for EOS.
            new AzureDBConn().put(new Offset(id, filter, topicPartition.partition(), offset));

        });
        return tweets;
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
