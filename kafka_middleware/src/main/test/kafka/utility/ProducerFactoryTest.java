package kafka.utility;

import kafka.model.Topic;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.PartitionInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class ProducerFactoryTest {

    Producer<String, String> producer;

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
        producer = ProducerFactory.getTweetProducer();
        List<PartitionInfo> infos = new ArrayList<>();
        assertTrue(producer.partitionsFor(Topic.TAG).size() > 0);
        assertTrue(producer.partitionsFor(Topic.MENTION).size() > 0);
        assertTrue(producer.partitionsFor(Topic.LOCATION).size() > 0);
    }

}