package kafka.model;


import jdk.internal.dynalink.beans.StaticClass;
import kafka.rest.SSERoutine;

import javax.jws.soap.SOAPBinding;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Singleton class for the Twitter model.
 * It mainly manages the users keeping them sorted by their last poll
 */
public class Twitter {

    private Future<?> sseCompleted;
    private ExecutorService sseRoutineThread;
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
        sseRoutineThread = Executors.newSingleThreadExecutor();
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

    public boolean existUser(String id) {
        User user = getUser(id);
        return user != null;
    }

    public Deque<User> getUsers() {
        return users;
    }

    /**
     * Check if the SSERoutine is done
     * @return true if the thread has ended otherwise false
     */
    public boolean isSSEDone() {
        return sseCompleted.isDone();
    }

    /**
     * Starts SSERoutine
     * @param timestamp Upper bound of the window
     */
    public void startSSE(long timestamp) {
        sseCompleted = sseRoutineThread.submit(new SSERoutine(timestamp));
    }

    public ExecutorService getSseRoutineThread() {
        return sseRoutineThread;
    }
}
