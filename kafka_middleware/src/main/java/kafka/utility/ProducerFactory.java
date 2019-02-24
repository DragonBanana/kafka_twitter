package kafka;

import kafka.partitioner.LocationPartitioner;
import kafka.partitioner.MentionPartitioner;
import kafka.partitioner.TagPartitioner;
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
    public static Producer getLocationProducer() {

        //Configuring the kafka producer
        Properties props = getDefaultProperty();

        //Configuring the custom partitioner
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, LocationPartitioner.class.getName());

        //Creating the producer
        Producer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }

    /**
     * Return the producer for tag topic.
     * @return the producer for tag topic.
     */
    public static Producer getTagProducer() {

        //Configuring the kafka producer
        Properties props = getDefaultProperty();

        //Configuring the custom partitioner
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, TagPartitioner.class.getName());

        //Creating the producer
        Producer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }

    /**
     * Return the producer for mention topic.
     * @return the producer for mention topic.
     */
    public static Producer getMentionProducer() {

        //Configuring the kafka producer
        Properties props = getDefaultProperty();

        //Configuring the custom partitioner
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MentionPartitioner.class.getName());

        //Creating the producer
        Producer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }

    /**
     * Return the default producer properties.
     * @return the default producer properties.
     */
    private static Properties getDefaultProperty() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

}
