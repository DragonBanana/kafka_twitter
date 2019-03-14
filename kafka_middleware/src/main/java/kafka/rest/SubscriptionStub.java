package kafka.rest;

import kafka.model.Subscription;
import kafka.model.User;

/**
 * Stub for the Subscription
 */
public class SubscriptionStub {

    private Subscription subscription;

    public SubscriptionStub(User user) {
        this.subscription = new Subscription(user);
    }

    public String followTag (String tag) {
        subscription.addTag(tag);
        return tag;
    }

    public String unfollowTag (String tag) {
        subscription.removeTag(tag);
        return tag;
    }

    public String followUser (String user) {
        subscription.addUser(user);
        return user;
    }

    public String unfollowUser(String user) {
        subscription.removeUser(user);
        return user;
    }

    public String followLocation(String location) {
        subscription.addLocation(location);
        return location;
    }

    public String unfollowLocation(String location) {
        subscription.removeLocation(location);
        return location;
    }

}
