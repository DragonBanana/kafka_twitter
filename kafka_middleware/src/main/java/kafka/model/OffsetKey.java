package kafka.model;

public class OffsetKey {

    /**
     * The user identifier.
     */
    private String user;

    /**
     * The filter used.
     */
    private String filter;

    /**
     * The topic partition of the offset.
     */
    private int topicPartition;

    /**
     * The constructor of the offset key.
     * @param user the user identifier.
     * @param filter the filter used for the search.
     * @param topicPartition the topic partition.
     */
    public OffsetKey(String user, String filter, int topicPartition) {
        this.user = user;
        this.filter = filter;
        this.topicPartition = topicPartition;
    }

    /**
     * Return the user identifier.
     * @return the user identifier.
     */
    public String getUser() {
        return user;
    }

    /**
     * Return the filter used.
     * @return the filter used.
     */
    public String getFilter() {
        return filter;
    }

    /**
     * Return the topic partition of the offset.
     * @return the topic partition of the offset.
     */
    public int getTopicPartition() {
        return topicPartition;
    }
}
