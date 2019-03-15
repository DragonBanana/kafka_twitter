package kafka.model;

import spark.Response;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

/**
 * Singleton class for the Twitter model.
 * It mainly manages the users keeping them sorted by their last poll
 */
public class Twitter {

    private static Twitter twitterSingleton;
    private Deque<User> users;

    public static Twitter getTwitter() {
        if (twitterSingleton == null) {
            twitterSingleton = new Twitter();
        }

        return twitterSingleton;
    }

    private Twitter() {
        users = new ArrayDeque<>();
    }

    public boolean createNewUser(String id) {

        for (User user : users) {
            if (user.getId().equals(id))
                return false;
        }

        User user = new User(id);

        //Set last poll to 0
        //TODO When register for the first time
        // will the user get last tweets from the last 5 minutes or all of them?
        user.getSubscriptionStub().updatePoll(0L);

        users.addFirst(user);

        return true;
    }


    public User getUser(String id) {
        for (User user : users) {
            if (user.getId().equals(id))
                return user;
        }
        return null;
    }

    public boolean existUser(Response response, String id) {
        User user = getUser(id);

        if (user == null) {
            response.status(404);
            response.body("User does not exist");
            return false;
        }
        return true;
    }
}
