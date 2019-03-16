package kafka.utility;

import kafka.model.Tweet;
import kafka.partitioner.TweetPartitioner;
import org.apache.avro.hadoop.io.AvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerFactory {

    /**
     * Return the producer for location topic.
     * @return the producer for location topic.
     */
    public static Producer<String, String> getTweetProducer() {

        //Configuring the kafka producer
        Properties props = getDefaultProperty();

        //Configuring the custom partitioner
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, TweetPartitioner.class.getName());

        //Creating the producer
        return new KafkaProducer<>(props);
    }

    /**
     * Return the default producer properties.
     * @return the default producer properties.
     */
    private static Properties getDefaultProperty() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.43.137:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
        return props;
    }

}
