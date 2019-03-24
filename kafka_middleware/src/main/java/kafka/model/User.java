package kafka.model;

import kafka.rest.SubscriptionStub;

public class User {

    /**
     * The identifier of the user.
     */
    private String id;

    /**
     * Contains the elements (tags, users and locations) followed
     * by the user.
     */
    private SubscriptionStub subscription;

    /**
     * Keeps track of connection for SSE
     */
    private VirtualClient virtualClient;

    /**
     * The constructor.
     * @param id the identifier of the user.
     */
    public User(String id) {
        this.id = id;
        subscription = new SubscriptionStub(this);
    }

    /**
     * The identifier of the user.
     * @return the identifier of the user.
     */
    public String getId() {
        return id;
    }

    /**
     * The user's subscriptions
     * @return the user's subscriptions
     */
    public SubscriptionStub getSubscriptionStub() {
        return subscription;
    }

    public VirtualClient getVirtualClient() {
        return virtualClient;
    }

    public void setVirtualClient(VirtualClient virtualClient) {
        this.virtualClient = virtualClient;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof User) {
            User user2 = (User) obj;
            System.out.println(user2.id + " " + this.id);
            return this.id.equals(user2.id);
        } else
            return false;
    }
}
