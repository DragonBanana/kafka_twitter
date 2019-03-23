package kafka.rest;

import com.google.gson.Gson;
import kafka.conf.ConsumerConfiguration;
import kafka.db.AzureDBConn;
import kafka.model.Offset;
import kafka.model.OffsetKey;
import kafka.model.Topic;
import kafka.utility.ConsumerFactory;
import kafka.utility.ProducerFactory;
import kafka.model.Tweet;
import kafka.utility.TopicPartitionFactory;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TweetStub {

    /**
     * Save the tweet.
     * @param tweet the tweet that has to be saved.
     * @return the tweet that has been saved.
     */

    public Tweet save(Tweet tweet) {

        //Get the filters used in the tweet.
        List<String> tweetFilters = tweet.getFilters();
        List<ProducerRecord<String, String>> records = new ArrayList<>();

        //Check where the tweet must be saved
        for (String filter : tweetFilters) {
            records.add(new ProducerRecord<>(filter, tweet.getLocation(),
                    new Gson().toJson(tweet, Tweet.class)));
        }
        System.out.println(tweetFilters);
        System.out.println(records);

        //Send data to Kafka

        records.forEach(record -> {
            Producer<String, String> producer = ProducerFactory.getTweetProducer();
            producer.initTransactions();
            try {
                producer.beginTransaction();
                producer.send(record);
                producer.commitTransaction();
            } catch (ProducerFencedException e) {
                producer.close();
            } catch (KafkaException e) {
                producer.abortTransaction();
            } finally {
                producer.close();
            }
        });


        return tweet;

    }

    /**
     * Main function for searching tweets given the filters.
     * @param id of requester.
     * @param locationFilters location filters.
     * @param tagFilters tag filters.
     * @param mentionFilters mention filters.
     * @return the latest tweet filtered using the filters params.
     */
    public List<Tweet> findTweets(String id, String locationFilters, String tagFilters, String mentionFilters){
        //TODO
        //taking out filters by locations, userFollowed and tags
        List<String> locationToFollow = Arrays.asList(locationFilters.split("&"));
        List<String> userToFollow = Arrays.asList(mentionFilters.split("&"));
        List<String> tagToFollow = Arrays.asList(tagFilters.split("&"));
        String filter = locationFilters + tagFilters + mentionFilters;

        if(!locationToFollow.isEmpty()){
            return filterByLocation(id, locationToFollow, userToFollow, tagToFollow, filter);
        }
        if(userToFollow.isEmpty()){
            //filter tweet using only tag.
            return findLatestByTag(id, tagToFollow, filter);

        }
        else
            //filter tweet using users mentioned (and tag if present).
            return findLatestByMention(id, userToFollow, tagToFollow, filter);
    }


    /**
     * Return the latest tweet filtered by location.
     * @param id the identifier of the requester.
     * @param locations the location filters.
     * @param users the user mentioned filters.
     * @param tags the tag filters.
     * @param filter the filters for the research.
     * @return the latest tweet filtered by location.
     */
    public List<Tweet> filterByLocation(String id, List<String> locations, List<String> users, List<String> tags, String filter) {
        //search in Topic.LOCATION, then filter result using users mentioned &/or tags.
        List<Tweet> tweets = new ArrayList<>();
        for (String loc: locations) {
            tweets.addAll(findLatestByLocation(id, loc, filter));
        }

        if(!users.isEmpty()){
            //filter result using users mentioned filter.
            tweets = tweets.stream()
                    .filter(tweet -> (tweet.getMentions()).containsAll(users))
                    .collect(Collectors.toList());
        }
        if(!tags.isEmpty()){
            //filter result using tags filter.
            tweets = tweets.stream()
                    .filter(tweet -> (tweet.getTags()).containsAll(tags))
                    .collect(Collectors.toList());
        }
        return tweets;
    }

    /**
     * Return the latest tweet filtered by location.
     * @param id the identifier of the requester.
     * @param location the location.
     * @param filter the filters for the research.
     * @return the latest tweet filtered by location.
     */
    public List<Tweet> findLatestByLocation(String id, String location, String filter) {
        //The topic we are reading from.
        String topic = Topic.LOCATION;
        //Getting the consumer.
        Consumer<String, String> consumer = ConsumerFactory.getConsumer();
        //Getting the partition of the topic.
        int partition = TopicPartitionFactory.getLocationPartition(location);
        //Creating the topic partition object (it is required in the next instructions).
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        //Subscribe to a topic.
        consumer.assign(Arrays.asList(topicPartition));
        //Getting the offset from the db.
        long offset = new AzureDBConn().get(new OffsetKey(id, filter, partition)).getValue().getOffset();
        //Moving the offset.
        consumer.seek(topicPartition, offset);
        consumer.poll(0);
        //Polling the data.
        ConsumerRecords<String,String> records = consumer.poll(100);
        //Transforming data and filtering. (!Only by location)
        List<Tweet> tweets = records.records(topicPartition).stream().map(record -> new Gson().fromJson(record.value(), Tweet.class)).filter(t -> t.getLocation().equals(location)).collect(Collectors.toList());
        //Getting the new offset.
        offset = consumer.position(topicPartition);
        //Saving the new offset for EOS.
        new AzureDBConn().put(new Offset(id, filter, partition, offset));
        //Returning the data.
        return tweets;
    }

    /**
     * Return the latest tweet filtered by tags.
     * @param id the identifier of the requester.
     * @param tags the list of tags.
     * @param filter the filters for the research.
     * @return the latest tweet filtered by tags.
     */
    public List<Tweet> findLatestByTag(String id, List<String> tags, String filter) {
        //The topic we are reading from.
        String topic = Topic.TAG;
        //Getting the consumer.
        Consumer<String, String> consumer = ConsumerFactory.getConsumer();
        //Declaring two partitions: the first for 1 tag search, the second for blob search.
        List<TopicPartition> topicPartitions = new ArrayList<>();
        if(tags.size() == 1) {
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
        topicPartitions.stream()
                .forEach(topicPartition -> {
                    //Getting the offset from the db.
                    long offset = new AzureDBConn().get(new OffsetKey(id, filter, topicPartition.partition())).getValue().getOffset();
                    //Moving the offset.
                    consumer.seek(topicPartition, offset);
                });
        //Polling the data.
        ConsumerRecords<String,String> records = consumer.poll(1000);
        //Transforming data and filtering. (!Only by tag)
        List<Tweet> tweets = new ArrayList();
        records.forEach(record -> {
            Tweet t = new Gson().fromJson(record.value(), Tweet.class);
            if(t.getTags().containsAll(tags))
                tweets.add(t);
        });
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
     * @param id the identifier of the requester.
     * @param mentions the user mentioned filters.
     * @param tags the tag filters.
     * @param filter the filters for the research.
     * @return the latest tweet filtered by user.
     */
    public List<Tweet> findLatestByMention(String id, List<String> mentions, List<String> tags, String filter) {

        //The topic we are reading from.
        String topic = Topic.MENTION;
        //Getting the consumer.
        Consumer<String, String> consumer = ConsumerFactory.getConsumer();
        //Declaring two partitions: the first for 1 mention search, the second for blob search.
        List<TopicPartition> topicPartitions = new ArrayList<>();
        if(mentions.size() == 1) {
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
        topicPartitions.stream()
                .forEach(topicPartition -> {
                    //Getting the offset from the db.
                    long offset = new AzureDBConn().get(new OffsetKey(id, filter, topicPartition.partition())).getValue().getOffset();
                    //Moving the offset.
                    consumer.seek(topicPartition, offset);
                });
        //Polling the data.
        ConsumerRecords<String,String> records = consumer.poll(1000);
        //Transforming data and filtering. (!Only by mention)
        final List<Tweet> tweets = new ArrayList();
        records.forEach(record -> {
            Tweet t = new Gson().fromJson(record.value(), Tweet.class);
            if(t.getMentions().containsAll(mentions))
                tweets.add(t);
        });
        topicPartitions.forEach(topicPartition -> {
            //Getting the new offset.
            long offset = consumer.position(topicPartition);
            //Saving the new offset for EOS.
            new AzureDBConn().put(new Offset(id, filter, topicPartition.partition(), offset));

        });
        if(tags.isEmpty())
            //Returning the data
            return tweets;

        //filter result using tags filter.
        List <Tweet> ts = tweets.stream()
                .filter(tweet -> (tweet.getTags()).containsAll(tags))
                .collect(Collectors.toList());
        return ts;
    }


}
