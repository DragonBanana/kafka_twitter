package kafka.rest;

import kafka.model.Twitter;
import kafka.model.User;

import java.util.Arrays;
import java.util.List;

import static spark.Spark.*;

public class SubscriptionRoute {

    public static void configureRoutes() {

        post("/subscription", (request, response) -> {
            response.type("application/json");
            String id = request.cookie("id");
            /**
             * body of the request containing the parameters to be followed
             */
            SubscriptionRequest sr = new Gson().fromJson(request.body(),SubscriptionRequest.class);


            Set<String> locationToFollow = sr.getLocations();
            Set<String> userToFollow = sr.getFollowedUsers();
            Set<String> tagToFollow = sr.getTags();


            //TODO Search for the user in the data structure
            User user = Twitter.getTwitter().getUser(id);

            if (user == null) {
                response.status(404);
                response.body("User does not exist");
                return null;
            }

            SubscriptionStub subscriptionStub = user.getSubscriptionStub();


            if (!locationToFollow.get(0).equals("*")) {
                locationToFollow.forEach(subscriptionStub::followLocation);
            }

            if (!userToFollow.get(0).equals("*")) {
                userToFollow.forEach(subscriptionStub::followUser);
            }

            if (!tagToFollow.get(0).equals("*")) {
                tagToFollow.forEach(subscriptionStub::followTag);
            }

            response.status(200);
            return "Subscriptions created";
        });
    }
}
