 package kafka.utility;

 import org.apache.kafka.clients.consumer.Consumer;
 import org.apache.kafka.clients.consumer.ConsumerConfig;
 import org.apache.kafka.clients.consumer.KafkaConsumer;
 import org.apache.kafka.common.serialization.StringDeserializer;

 import java.util.Properties;

public class ConsumerFactory {

    /**
     * Return the consumer.
     * @return the consumer.
     */
    public static Consumer<String, String> getConsumer() {
        return new KafkaConsumer<>(getDefaultProperty());
    }

    /**
     * Return the default producer properties.
     * @return the default producer properties.
     */
    private static Properties getDefaultProperty() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "23.97.231.221:32788");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100000");
       // props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-client");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }

}
