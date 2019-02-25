package kafka.model;

public class User {

    /**
     * The identifier of the user.
     */
    private String id;

    /**
     * The constructor.
     * @param id the identifier of the user.
     */
    public User(String id) {
        this.id = id;
    }

    /**
     * The identifier of the user.
     * @return the identifier of the user.
     */
    public String getId() {
        return id;
    }
}
