package kafka.partitioner;

import com.google.gson.Gson;
import kafka.model.Topic;
import kafka.model.Tweet;
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

        Tweet tweet = new Gson().fromJson((String) value, Tweet.class);

        if(topic.equals(Topic.LOCATION)){
            String location = tweet.getLocation();
            int hashLocation = location.hashCode();
            System.out.println("location partition " + (Math.abs(hashLocation)) % (numPartitions));
            return (Math.abs(hashLocation)) % (numPartitions);
        }
        if(topic.equals(Topic.MENTION)){
            if (tweet.getMentions().size() == 1){
                String mention = tweet.getMentions().get(0);
                int hashMention = mention.hashCode();
                System.out.println("mention partition " + (Math.abs(hashMention))%(numPartitions-1));
                return (Math.abs(hashMention))%(numPartitions-1);
            }
            return numPartitions - 1;
        }
        if(topic.equals(Topic.TAG)){
            if (tweet.getTags().size() == 1){
                String tag = tweet.getTags().get(0);
                int hashTag = tag.hashCode();
                System.out.println("tag partition " + (Math.abs(hashTag))%(numPartitions-1));
                return (Math.abs(hashTag))%(numPartitions-1);
            }
            return numPartitions - 1;
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
