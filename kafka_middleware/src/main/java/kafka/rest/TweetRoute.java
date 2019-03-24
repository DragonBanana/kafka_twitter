package kafka.rest;

import com.google.gson.Gson;
import kafka.model.Tweet;
import kafka.model.Twitter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static spark.Spark.*;

public class TweetRoute {

    public static void configureRoutes() {

        post("/tweets", (request, response) -> {

            String id = request.cookie("id");
            //Search for the user in the data structure
            if (!Twitter.getTwitter().existUser(id)) {
                response.status(404);
                return "User does not exist. Sign in if you want to post a tweet";
            }

            response.type("application/json");
            response.status(200);
            Tweet tweet = new Gson().fromJson(request.body(), Tweet.class);
            new TweetStub().save(tweet);
            return "Tweet created" + new Gson().toJson(tweet);
        });

        get("/tweets/*/*/*/latest",(request, response) -> {

            String id = request.cookie("id");

            //Search for the user in the data structure
            if (!Twitter.getTwitter().existUser(id)) {
                response.status(404);
                return "User does not exist. Sign in if you want to get the tweets";
            }

            response.type("application/json");
            response.status(200);

            List<String> locations = Arrays.asList(request.splat()[0].split("&"));
            List<String> tags = Arrays.asList(request.splat()[1].split("&"));
            List<String> mentions = Arrays.asList(request.splat()[2].split("&"));

            return new Gson().toJson(new TweetStub().findTweets(id, locations, tags, mentions));
            //TODO check error in filters
        });
    }
}
