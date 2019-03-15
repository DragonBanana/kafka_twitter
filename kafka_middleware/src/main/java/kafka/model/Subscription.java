package kafka.model;

import java.util.*;

/**
 * Class used to keep track which tags, followedUsers and locations
 * are followedUsers by the user.
 */
public class Subscription {

    /**
     * Last poll of the user.
     */
    private Long lastPoll;

    /**
     * Taags followedUsers by the user
     */
    private Set<String> tags = new HashSet<>();

    /**
     * Users followed
     */
    private Set<String> followedUsers = new HashSet<>();

    /**
     * Locations followed
     */
    private Set<String> locations = new HashSet<>();

    public Subscription(User user) {

        //TODO use username instead of id
        //A user can always see its own tweets
        followedUsers.add(user.getId());
    }

    public Set<String> getTags() {
        return tags;
    }

    public boolean addTag(String tag){
        return tags.add(tag);
    }

    public boolean removeTag(String tag) {
        return tags.remove(tag);
    }

    public Set<String> getFollowedUsers() { return followedUsers; }

    public boolean addUser(String user) {
        return followedUsers.add(user);
    }

    public boolean removeUser(String user) {
        return followedUsers.remove(user);
    }

    public Set<String> getLocations() { return locations; }

    public boolean addLocation(String location) {
        return locations.add(location);
    }

    public boolean removeLocation(String location) {
        return locations.remove(location);
    }

    public Long getLastPoll() { return lastPoll; }

    public void setLastPoll(Long lastPoll) { this.lastPoll = lastPoll; }
}
