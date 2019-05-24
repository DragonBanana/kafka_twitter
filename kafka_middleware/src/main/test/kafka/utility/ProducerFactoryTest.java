package kafka.utility;

import kafka.model.Topic;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class ProducerFactoryTest {

    /**
     * Asking to Kafka if it has the three topic (tag, mention and location)
     */
    @Test
    public void getConsumer() {
        Producer<String, String> producer = ProducerFactory.getTweetProducer();
        System.out.println(producer.partitionsFor(Topic.TAG).size());
        assertTrue(producer.partitionsFor(Topic.TAG).size() > 0);
        assertTrue(producer.partitionsFor(Topic.MENTION).size() > 0);
        assertTrue(producer.partitionsFor(Topic.LOCATION).size() > 0);
    }

}