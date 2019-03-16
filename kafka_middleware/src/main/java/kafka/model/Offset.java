package kafka.model;

public class Offset {

    private OffsetKey key;
    private OffsetValue value;

    public Offset(OffsetKey key, OffsetValue value) {
        this.key = key;
        this.value = value;
    }

    public OffsetKey getKey() {
        return key;
    }

    public OffsetValue getValue() {
        return value;
    }
}
