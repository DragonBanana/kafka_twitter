package kafka.rest;

import kafka.model.Subscription;
import kafka.model.User;

import java.util.ArrayList;
import java.util.List;

/**
 * Stub for the Subscription
 */
public class SubscriptionStub {

    private Subscription subscription;

    public SubscriptionStub(User user) {
        this.subscription = new Subscription(user);
    }

    String followTag(String tag) {
        subscription.addTag(tag);
        return tag;
    }

    String unfollowTag(String tag) {
        subscription.removeTag(tag);
        return tag;
    }

    String followUser(String user) {
        subscription.addUser(user);
        return user;
    }

    String unfollowUser(String user) {
        subscription.removeUser(user);
        return user;
    }

    String followLocation(String location) {
        subscription.addLocation(location);
        return location;
    }

    String unfollowLocation(String location) {
        subscription.removeLocation(location);
        return location;
    }

    Long lastPoll() {
        return subscription.getLastPoll();
    }

    public Long updatePoll(Long newPoll) {
        subscription.setLastPoll(newPoll);
        return newPoll;
    }

    List<String> getLocationsFollowed() {
        return new ArrayList<>(subscription.getLocations());
    }

    List<String> getTagsFollowed() {
        return new ArrayList<>(subscription.getTags());
    }

    List<String> getUsersFollowed() {
        return new ArrayList<>(subscription.getFollowedUsers());
    }


}
