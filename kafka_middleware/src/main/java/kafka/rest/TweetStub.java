package kafka.rest;

import com.google.gson.Gson;
import kafka.conf.ConsumerConfiguration;
import kafka.utility.ConsumerFactory;
import kafka.utility.ProducerFactory;
import kafka.model.Tweet;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

public class TweetStub {

    /**
     * Save the tweet.
     * @param tweet the tweet that has to be saved.
     * @return the tweet that has been saved.
     */
    public Tweet save(Tweet tweet) {

        //Creating producer and producer record
        Producer<String, String> producer = ProducerFactory.getTweetProducer();
        ProducerRecord record = new ProducerRecord<>("tweet", tweet.getAuthor(), new Gson().toJson(tweet, Tweet.class));

        //Send data to Kafka
        producer.send(record);
        producer.flush();
        producer.close();
        return tweet;

    }

    /**
     * Main function for searching tweets given the filters.
     * @param id of requester.
     * @param filters.
     * @return the latest tweet filtered using the filters param.
     */
    //TODO choose how to save the filters.
    //public List<Tweet> findTweets(String id,  f){
        //TODO
        //return null
    //}



    /**
     * Return the latest tweet filtered by location.
     * @param id the identifier of the requester.
     * @param location the location filter.
     * @return the latest tweet filtered by location.
     */
    public List<Tweet> findLatestByLocation(String id, String location) {
        //TODO
        return null;
    }

}
