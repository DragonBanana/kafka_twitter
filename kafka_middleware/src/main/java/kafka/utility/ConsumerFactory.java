package kafka.utility;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class ConsumerFactory {

    /**
     * Return the consumer for location topic.
     * @return the consumer for location topic.
     */
    public static Consumer getLocationTweetConsumer(String user, String location) {
        //TODO
        return null;
    }

    /**
     * Return the default producer properties.
     * @return the default producer properties.
     */
    private static Properties getDefaultProperty() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "false");
        return props;
    }

}
