package kafka.utility;

public class TopicPartitionFactory {

    //TODO non so se serve.

    /**
     * The number of partitions in 'location' topic.
     */
    public static final int LOCATION_PARTITIONS = 60;

    /**
     * Return the partition of the location topic given a string.
     * @param location the input key.
     * @return the partition of the location.
     */
    public static int getLocationPartition(String location) {
        return Math.abs(location.hashCode()) % TopicPartitionFactory.LOCATION_PARTITIONS;
    }

}
