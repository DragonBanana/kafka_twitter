package kafka.model;

public class Offset {

    /**
     * The key of the offset.
     */
    private OffsetKey key;

    /**
     * the value of the offset.
     */
    private OffsetValue value;

    /**
     * The constructor of the offset.
     * @param user the user identifier.
     * @param filter the filter used for the search.
     * @param topicPartition the topic partition.
     * @param offset the offset value.
     */
    public Offset(String user, String filter, int topicPartition, long offset) {
        this.key = new OffsetKey(user, filter, topicPartition);
        this.value = new OffsetValue(offset);
    }

    /**
     * The constructor of the offset.
     * @param key the key of the offset.
     * @param value the value of the offset.
     */
    public Offset(OffsetKey key, OffsetValue value) {
        this.key = key;
        this.value = value;
    }

    /**
     * Return the key of the offset.
     * @return the key of the offset.
     */
    public OffsetKey getKey() {
        return key;
    }

    /**
     * Return the value of the offset.
     * @return the value of the offset.
     */
    public OffsetValue getValue() {
        return value;
    }
}
