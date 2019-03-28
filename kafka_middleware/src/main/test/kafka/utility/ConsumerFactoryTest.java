package kafka.utility;

import kafka.model.Topic;
import kafka.utility.ConsumerFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class ConsumerFactoryTest {

    Consumer<String, String> consumer;

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    /**
     * Asking to Kafka if it has the three topic (tag, mention and location)
     */
    @Test
    public void getConsumer() {
        consumer = ConsumerFactory.getConsumer();
        List<PartitionInfo> infos = new ArrayList<>();
        consumer.listTopics().values().stream().forEach(list -> infos.addAll(list));
        List<String> topics = infos.stream().map(PartitionInfo::topic).collect(Collectors.toList());
        assertTrue(topics.contains(Topic.TAG));
        assertTrue(topics.contains(Topic.MENTION));
        assertTrue(topics.contains(Topic.LOCATION));
    }
}