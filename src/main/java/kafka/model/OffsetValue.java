package kafka.model;

public class OffsetValue {

    private long offset;

    public OffsetValue(long offset) {
        this.offset = offset;
    }

    public long getOffset() {
        return offset;
    }
}
