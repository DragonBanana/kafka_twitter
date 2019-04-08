package kafka.rest;

import kafka.model.Twitter;

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

            Twitter twitter = Twitter.getTwitter();
            //response.header("Access-Control-Allow-Origin", "http://"+"127.0.0.1"+":"+"4567" + "/*");
            //response.header("Access-Control-Allow-Origin", "*");
            response.header("Access-Control-Allow-Credentials", "true");
            //response.header("Set-Cookie", "id="+ id  + "; path=\"/tuamadre\"");
            response.cookie("/", "id", id, 10000,false, false);
//            response.cookie("id", id, -1, false, false);

            if (twitter.createNewUser(id)) {
                response.status(200);
                return "{\"type\" : \"success\", \"message\" : \"id="+id+"\"}";
            }
            else {
                response.status(400);
                return "{\"type\" : \"error\", \"message\" : \"user does not exist, sign in if you want to post a tweet\"}";
            }
        });

    }
}
