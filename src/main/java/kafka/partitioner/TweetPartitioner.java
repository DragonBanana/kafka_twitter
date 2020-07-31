package kafka.partitioner;

import kafka.model.Topic;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class TweetPartitioner implements Partitioner {


    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        String keyTweet = (String) key;

        if (topic.equals(Topic.LOCATION)) {
            int hashLocation = keyTweet.hashCode();
            System.out.println("location partition " + (Math.abs(hashLocation)) % (numPartitions));
            return (Math.abs(hashLocation)) % (numPartitions);
        }
        if (topic.equals(Topic.MENTION)) {
            int hashMention = keyTweet.hashCode();
            System.out.println("mention partition " + (Math.abs(hashMention)) % (numPartitions));
            return (Math.abs(hashMention)) % (numPartitions);

        }
        if (topic.equals(Topic.TAG)) {
            int hashTag = keyTweet.hashCode();
            System.out.println("tag partition " + (Math.abs(hashTag)) % (numPartitions));
            return (Math.abs(hashTag)) % (numPartitions);
        }
        return 1;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
