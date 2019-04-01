package kafka.rest;

import kafka.model.Twitter;
import kafka.model.User;

import static spark.Spark.post;

/**
 * Routes regarding users management
 */
public class UserRoute {

    /**
     * POST /users/:id API used for both login and registration to Twitter.
     *
     * Search in the registered users if already exist one with the requested id.
     * If it does not exist create a new user otherwise log the user into the system.
     * In both cases attach a cookie in the response.
     */
    public static void configureRoutes() {
        post("/users/:id", (request, response) -> {

            response.type("application/json");
            response.status(200);

            String id = request.params(":id");

            User user;
            Twitter twitter = Twitter.getTwitter();
            response.cookie("id", id);

            if (twitter.createNewUser(id)) {
                return "New user created with id: " + id;
            }
            else {
                return "User already registered with id: " + id;
            }
        });

    }
}
