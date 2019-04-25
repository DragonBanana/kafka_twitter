package kafka.utility;

import java.util.HashSet;
import java.util.Set;

public class SubscriptionRequest{
    /**
     * Tags to be followed in the SubscriptionRequest
     */
    private Set<String> tags = new HashSet<>();

    /**
     * Users to be followed in the SubscriptionRequest
     */
    private Set<String> followedUsers = new HashSet<>();

    /**
     * Locations to be followed in the SubscriptionRequest
     */
    private Set<String> locations = new HashSet<>();

    public Set<String> getFollowedUsers() {
        return followedUsers;
    }

    public Set<String> getLocations() {
        return locations;
    }

    public Set<String> getTags() {
        return tags;
    }

    public void setTags(Set<String> tags) { this.tags = tags; }

    public void setFollowedUsers(Set<String> followedUsers) { this.followedUsers = followedUsers; }

    public void setLocations(Set<String> locations) { this.locations = locations; }
}