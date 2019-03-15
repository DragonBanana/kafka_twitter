package kafka.rest;

import kafka.model.Twitter;
import kafka.model.User;

import java.util.Arrays;
import java.util.List;

import static spark.Spark.*;

public class SubscriptionRoute {

    public static void configureRoutes() {

        post("/subscription/:location/:user/:tag", (request, response) -> {
            response.type("application/json");
            String id = request.cookie("id");

            //TODO Check what char use as separator. Temporary char: &
            List<String> locationToFollow = Arrays.asList(request.params(":location").split("&"));
            List<String> userToFollow = Arrays.asList(request.params(":user").split("&"));
            List<String> tagToFollow = Arrays.asList(request.params(":tag").split("&"));


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
