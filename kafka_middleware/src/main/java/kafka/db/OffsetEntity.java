package kafka.db;

import com.microsoft.azure.storage.table.TableServiceEntity;
import kafka.model.Offset;
import kafka.model.OffsetKey;
import kafka.model.OffsetValue;

public class OffsetEntity extends TableServiceEntity {

    private String user;
    private String filter;
    private int topicPartition;
    private Long offset;

    public OffsetEntity() {

    }

    public OffsetEntity(Offset offset) {
        this.partitionKey = offset.getKey().getUser();
        this.rowKey = offset.getKey().getUser()+offset.getKey().getTopicPartition();
        this.user = offset.getKey().getUser();
        this.filter = offset.getKey().getFilter();
        this.topicPartition = offset.getKey().getTopicPartition();
        this.offset = offset.getValue().getOffset();
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public Offset asOffset() {
        return new Offset(new OffsetKey(user, filter, topicPartition), new OffsetValue(offset));
    }
}
