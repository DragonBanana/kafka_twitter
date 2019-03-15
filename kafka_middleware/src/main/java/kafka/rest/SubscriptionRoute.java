package kafka.rest;

import com.google.gson.Gson;
import kafka.model.Twitter;
import kafka.model.User;
import kafka.utility.SubscriptionRequest;

import java.util.Set;

import static spark.Spark.*;

public class SubscriptionRoute {

    public static void configureRoutes() {

        post("/subscription", (request, response) -> {
            response.type("application/json");
            String id = request.cookie("id");

            /*
              body of the request containing the parameters to be followed
             */
            SubscriptionRequest sr = new Gson().fromJson(request.body(), SubscriptionRequest.class);


            Set<String> locationToFollow = sr.getLocations();
            Set<String> userToFollow = sr.getFollowedUsers();
            Set<String> tagToFollow = sr.getTags();

            Twitter twitter = Twitter.getTwitter();

            //Search for the user in the data structure
            if (twitter.existUser(response, id)) return null;

            SubscriptionStub subscriptionStub = twitter.getUser(id).getSubscriptionStub();


            if (!locationToFollow.isEmpty()) {
                locationToFollow.forEach(subscriptionStub::followLocation);
            }

            if (!userToFollow.isEmpty()) {
                userToFollow.forEach(subscriptionStub::followUser);
            }

            if (!tagToFollow.isEmpty()) {
                tagToFollow.forEach(subscriptionStub::followTag);
            }

            response.status(200);
            return "Subscriptions created";
        });
    }
}
