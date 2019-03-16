 package kafka.utility;

import kafka.model.Offset;
import kafka.model.OffsetKey;
import kafka.rest.OffsetStub;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class ConsumerFactory {

    /**
     * Return the consumer topic.
     * @return the consumer topic.
     */
    public static Consumer<String, String> getConsumer() {
        return new KafkaConsumer(getDefaultProperty());
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
