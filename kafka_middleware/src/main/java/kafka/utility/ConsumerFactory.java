 package kafka.utility;

 import kafka.model.Topic;
 import org.apache.kafka.clients.consumer.Consumer;
 import org.apache.kafka.clients.consumer.ConsumerConfig;
 import org.apache.kafka.clients.consumer.KafkaConsumer;
 import org.apache.kafka.common.serialization.StringDeserializer;

 import java.time.Duration;
 import java.util.*;

 public class ConsumerFactory {

    /**
     * The number of consumer per group.
     */
    private static final int N_CONSUMER = 3;

    private static List<Consumer<String, String>> locationSubscribers;
    private static List<Consumer<String, String>> tagSubscribers;
    private static List<Consumer<String, String>> mentionSubscribers;

    /**
     * Return the consumer.
     * @return the consumer.
     */
    public static Consumer<String, String> getConsumer() {
        return new KafkaConsumer<>(getDefaultProperty());
    }

    /**
     * Returns a list of consumers in the same consumer group.
     * @param groupId the consumer group id
     * @return a list of consumers in the same consumer group.
     */
    public static List<Consumer<String, String>> getConsumerGroup(String topic, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "23.97.231.221:32782");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100000");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        List<Consumer<String, String>> consumerList = new ArrayList<>();
        for(int i = 0; i < N_CONSUMER; i++) {
            consumerList.add(new KafkaConsumer<>(props));
        }
        consumerList.parallelStream().forEach(c -> {
            c.subscribe(Collections.singletonList(topic));
            c.poll(Duration.ofMillis(1000));
        });
        return consumerList;
    }

     /**
      * Returns a list of consumers in the same consumer group.
      * @return a list of consumers in the same consumer group.
      */
     public static List<Consumer<String, String>> getSubscribeConsumerGroup(String topic) {
         if(topic.equals(Topic.LOCATION)) {
             if (locationSubscribers == null) {
                 locationSubscribers = getConsumerGroup(topic, "loc-subscriber-group");
             }
             return locationSubscribers;
         } else if(topic.equals(Topic.TAG)) {
             if (tagSubscribers == null) {
                 tagSubscribers = getConsumerGroup(topic, "tag-subscriber-group");
             }
             return tagSubscribers;
         } else if(topic.equals(Topic.MENTION)) {
             if (mentionSubscribers == null) {
                 mentionSubscribers = getConsumerGroup(topic, "men-subscriber-group");
             }
             return mentionSubscribers;
         }
         return null;
     }

     /**
      * Returns a list of consumers in the same consumer group.
      * @return a list of consumers in the same consumer group.
      */
     public static List<Consumer<String, String>> getAllSubscribeConsumerGroup() {
         List<Consumer<String, String>> list = new ArrayList<>();
         list.addAll(locationSubscribers);
         list.addAll(tagSubscribers);
         list.addAll(mentionSubscribers);
         return list;
     }

    /**
     * Return the default producer properties.
     * @return the default producer properties.
     */
    private static Properties getDefaultProperty() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "23.97.231.221:32782");
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
