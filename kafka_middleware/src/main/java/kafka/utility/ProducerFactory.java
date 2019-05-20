package kafka.utility;

import kafka.partitioner.TweetPartitioner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerFactory {

    /**
     * Return the producer.
     * @return the producer.
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
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "23.97.231.221:32777");
        //TODO Check what transactional id has to be assigned
        String transactionId = Double.toString(Math.abs(Math.random()));
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "100000");
        return props;
    }

}
