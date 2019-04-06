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

            String id = request.params(":id");

            User user;
            Twitter twitter = Twitter.getTwitter();
            response.cookie("id", id);

            if (twitter.createNewUser(id)) {
                response.status(200);
                return "{\"type\" : \"success\", \"message\" : \"user with " + id + " created}\"}";
            }
            else {
                response.status(400);
                return "{\"type\" : \"error\", \"message\" : \"user does not exist, sign in if you want to post a tweet\"}";
            }
        });

    }
}
