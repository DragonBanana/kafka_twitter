package kafka.utility;

import kafka.model.Topic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConsumerFactoryTest {

    /**
     * Asking to Kafka if it has the three topic (tag, mention and location)
     */
    @Test
    public void getConsumer() {
        Consumer<String, String> consumer = ConsumerFactory.getConsumer();
        List<PartitionInfo> infos = new ArrayList<>();
        consumer.listTopics().values().forEach(infos::addAll);
        List<String> topics = infos.stream().map(PartitionInfo::topic).collect(Collectors.toList());
        assertTrue(topics.contains(Topic.TAG));
        assertTrue(topics.contains(Topic.MENTION));
        assertTrue(topics.contains(Topic.LOCATION));
    }

    /**
     * Getting a consumer group
     */
    @Test
    public void getConsumers() {
        for(int i = 0; i < 5; i++) {
            List<Consumer<String, String>> consumers = ConsumerFactory.getConsumerGroup("location", "group-0");
            List<Integer> list = consumers
                    .stream()
                    .map(c -> c.assignment()
                            .stream()
                            .mapToInt(a -> a.partition())
                            .boxed()
                            .collect(Collectors.toList()))
                    .reduce((l1, l2) -> {
                        l1.addAll(l2);
                        return l1;
                    })
                    .get();
            assertEquals(list.stream().distinct().count(), list.size());
            consumers
                    .forEach(c -> c.assignment()
                            .forEach(a -> assertEquals("location", a.topic())));
        }
    }
}