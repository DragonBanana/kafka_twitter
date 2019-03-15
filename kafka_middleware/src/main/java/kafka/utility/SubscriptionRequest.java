package kafka.rest;

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
}