package kafka.db;

import com.microsoft.azure.storage.table.TableServiceEntity;
import kafka.model.Offset;
import kafka.model.OffsetKey;
import kafka.model.OffsetValue;

public class OffsetEntity extends TableServiceEntity {

    private String user;
    private String filter;
    private Long offset;

    public OffsetEntity() {

    }

    public OffsetEntity(Offset offset) {
        this.partitionKey = offset.getKey().getUser();
        this.rowKey = offset.getKey().getFilter();
        this.user = offset.getKey().getUser();
        this.filter = offset.getKey().getFilter();
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
        return new Offset(new OffsetKey(user, filter), new OffsetValue(offset));
    }
}
