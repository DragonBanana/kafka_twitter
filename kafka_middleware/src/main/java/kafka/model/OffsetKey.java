package kafka.model;

public class OffsetKey {

    private String user;
    private String filter;

    public OffsetKey(String user, String filter) {
        this.user = user;
        this.filter = filter;
    }

    public String getUser() {
        return user;
    }

    public String getFilter() {
        return filter;
    }
}
