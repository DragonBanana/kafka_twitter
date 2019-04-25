package kafka.utility;

public class TopicPartitionFactory {

    //TODO non so se serve.

    /**
     * The number of partitions in 'location' topic.
     */
    public static final int LOCATION_PARTITIONS = 60;

    /**
     * The number of partitions in 'tag' topic.
     */
    public static final int TAG_PARTITIONS = 60;

    /**
     * The partition of the blob storage, where the tweets with composed tags are inserted.
     */
    public static final int TAG_PARTITION_BLOB = 59;

    /**
     * The number of partitions in 'mention' topic.
     */
    public static final int MENTION_PARTITIONS = 60;


    /**
     * The partition of the blob storage, where the tweets with composed mentions are inserted.
     */
    public static final int MENTION_PARTITION_BLOB = 59;

    /**
     * Return the partition of the location topic given a string.
     * @param location the input key.
     * @return the partition of the location.
     */
    public static int getLocationPartition(String location) {
        return Math.abs(location.hashCode()) % TopicPartitionFactory.LOCATION_PARTITIONS;
    }

    public static int getTagPartition(String tag) {
        return Math.abs(tag.hashCode()) % (TopicPartitionFactory.TAG_PARTITIONS - 1);
    }

    public static int getMentionPartition(String mention) {
        return Math.abs(mention.hashCode()) % (TopicPartitionFactory.MENTION_PARTITIONS - 1);
    }


}
