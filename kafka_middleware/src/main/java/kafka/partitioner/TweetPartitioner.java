package kafka.partitioner;

import com.google.gson.Gson;
import kafka.model.Tweet;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class TweetPartitioner implements Partitioner {


    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //TODO
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        Tweet tweet = new Gson().fromJson((String) value, Tweet.class);

        String author = tweet.getAuthor();
        int hashAuthor = author.hashCode();
        if(topic.equals("location")){
            String location = tweet.getLocation();
            int hashLocation = location.hashCode();
            return (hashAuthor + hashLocation)%(numPartitions-1);
        }
        if(topic.equals("mention")){
            if (tweet.getMentions().size() == 1){
                String mention = tweet.getMentions().get(0);
                int hashMention = mention.hashCode();
                return (hashAuthor + hashMention)%(numPartitions-1);
            }
            return numPartitions;
        }
        if(topic.equals("tag")){
            if (tweet.getTags().size() == 1){
                String tag = tweet.getTags().get(0);
                int hashTag = tag.hashCode();
                return (hashAuthor + hashTag)%(numPartitions-1);
            }
            return numPartitions;
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
