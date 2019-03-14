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
}
