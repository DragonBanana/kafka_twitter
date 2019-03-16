package kafka.rest;

import com.google.gson.Gson;
import kafka.conf.ConsumerConfiguration;
import kafka.db.AzureDBConn;
import kafka.model.Offset;
import kafka.model.OffsetKey;
import kafka.utility.ConsumerFactory;
import kafka.utility.ProducerFactory;
import kafka.model.Tweet;
import kafka.utility.TopicPartitionFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class TweetStub {


    public static void main(String[] args) {
        TweetStub tweetStub = new TweetStub();
        List<String> tags = new ArrayList<>();
        tags.add("#swag");
        List<String> mentions = new ArrayList<>();
        mentions.add("@bellofigo");
        tweetStub.save(new Tweet("luca", "Hello from the stub", "now","verona", tags, mentions ));
    }

    /**
     * Save the tweet.
     * @param tweet the tweet that has to be saved.
     * @return the tweet that has been saved.
     */

    public Tweet save(Tweet tweet) {

        //Creating producer
        Producer<String, String> producer = ProducerFactory.getTweetProducer();

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
        records.forEach(producerRecord -> {
            producer.send(producerRecord);
        });

        producer.flush();
        producer.close();
        return tweet;

    }

    /**
     * Main function for searching tweets given the filters.
     * @param id of requester.
     * @param filters .
     * @return the latest tweet filtered using the filters param.
     */
    public List<Tweet> findTweets(String id, String filters){
        //TODO
        //taking out filters by locations, userFollowed and tags
        String [] parts0 = filters.split("location=");
        String [] parts1 = parts0[1].split("user=");
        String [] parts2 = parts1[1].split("tags=");
        String location = parts1[0];
        String user = parts2[0];
        String tag = parts2[1];
        List<String> locationToFollow = Arrays.asList(location.split("&"));
        List<String> userToFollow = Arrays.asList(user.split("&"));
        List<String> tagToFollow = Arrays.asList(tag.split("&"));
        if(!locationToFollow.isEmpty()){
            return findLatestByLocation(id, locationToFollow, userToFollow, tagToFollow);
        }
        if(tagToFollow.isEmpty()){
            return findLatestByUserFollowed(id, userToFollow);
        }
        else
            return findLatestByTag(id, tagToFollow);
    }

    /**
     * Return the latest tweet filtered by location.
     * @param id the identifier of the requester.
     * @param location the location.
     * @return the latest tweet filtered by location.
     */
    public List<Tweet> findLatestByLocation(String id, String location, String filter) {
        try {
            //The topic we are reading from.
            String topic = "location";
            //Getting the consumere.
            Consumer<String, String> consumer = ConsumerFactory.getConsumer();
            //Subscribe to a topic.
            consumer.subscribe(Arrays.asList(topic));
            //Getting the partition of the topic.
            int partition = TopicPartitionFactory.getLocationPartition(location);
            //Creating the topic partition object (it is required in the next instructions).
            TopicPartition topicPartition = new TopicPartition(topic, partition);
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
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * Return the latest tweet filtered by location.
     * @param id the identifier of the requester.
     * @param locations the location filters.
     * @return the latest tweet filtered by location.
     */
    public List<Tweet> findLatestByLocation(String id, List<String> locations, List<String> users, List<String> tags) {
        //TODO search in location Topic, then filter result using users &/or tags
        for (String loc: locations) {
            //Consumer<String, String> consumer = ConsumerFactory.getConsumer(id,loc);
            //int partition = TopicPartitionFactory.getLocationPartition(loc);
            //consumer.subscribe(Arrays.asList("location"));
            //TODO polling
        }
        if(!users.isEmpty()){
            //TODO filter result using users filters
        }
        if(!tags.isEmpty()){
            //TODO filter result using tags filters
        }
        return null;
    }

    /**
     * Return the latest tweet filtered by userFollowed.
     * @param id the identifier of the requester.
     * @param users the user filters.
     * @return the latest tweet filtered by user.
     */
    public List<Tweet> findLatestByUserFollowed(String id, List<String> users) {
        //TODO
        return null;
    }

    /**
     * Return the latest tweet filtered by tag.
     * @param id the identifier of the requester.
     * @param tags the tag filters.
     * @return the latest tweet filtered by tag.
     */
    public List<Tweet> findLatestByTag(String id, List<String> tags) {
        //TODO
        return null;
    }
}
