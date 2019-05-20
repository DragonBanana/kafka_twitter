package kafka.rest;

import com.google.gson.Gson;
import kafka.model.Twitter;
import kafka.utility.SubscriptionRequest;

import java.util.Set;

import static spark.Spark.post;

class SubscriptionRoute {

    static void configureRoutes() {

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
            if (!Twitter.getTwitter().existUser(id)) {
                response.status(404);
                return "User does not exist. Sign in if you want to post a tweet";
            }

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
            response.header("Access-Control-Allow-Origin", "*");
            return new Gson().toJson(subscriptionStub);
        });
    }
}
