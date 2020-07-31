package kafka.utility;

import kafka.model.Topic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

public class ConsumerFactoryTest {

    /**
     * Asking to Kafka if it has the three topic (tag, mention and location)
     */
    @Test
    public void getConsumer() {
        Consumer<String, String> consumer = ConsumerFactory.getConsumer("test-group");
        List<PartitionInfo> infos = new ArrayList<>();
        consumer.listTopics().values().forEach(infos::addAll);
        List<String> topics = infos.stream().map(PartitionInfo::topic).collect(Collectors.toList());
        assertTrue(topics.contains(Topic.TAG));
        assertTrue(topics.contains(Topic.MENTION));
        assertTrue(topics.contains(Topic.LOCATION));
    }
}