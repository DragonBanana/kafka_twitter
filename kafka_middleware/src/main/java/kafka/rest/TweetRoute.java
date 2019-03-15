package kafka.rest;

import com.google.gson.Gson;
import kafka.model.Tweet;
import kafka.model.Twitter;
import kafka.model.User;
import kafka.utility.SubscriptionRequest;
import spark.Response;


import static spark.Spark.*;

public class TweetRoute {

    public static void configureRoutes() {

        post("/tweets", (request, response) -> {

            String id = request.cookie("id");
            //Search for the user in the data structure
            if (!Twitter.getTwitter().existUser(response, id)) return null;
            response.type("application/json");
            response.status(200);
            Tweet tweet = new Gson().fromJson(request.body(), Tweet.class);
            new TweetStub().save(tweet);
            return "Tweet created" + new Gson().toJson(tweet);
        });

        get("/tweets/{filter}/latest",(request, response) -> {

            String id = request.cookie("id");
            //Search for the user in the data structure
            if (!Twitter.getTwitter().existUser(response, id)) return null;
            response.type("application/json");
            response.status(200);

            //TODO how to save the filter from the request
            //should we create a class o just a collection?
            //new TweetStub().findTweets(id,filter);
            return null;
        });
    }
}
