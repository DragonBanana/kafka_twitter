package kafka.rest;

import com.google.gson.Gson;
import kafka.model.Tweet;
import kafka.model.Twitter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static spark.Spark.get;
import static spark.Spark.post;

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
            return new Gson().toJson(tweet);
        });

        get("/tweets/*/*/*/latest", (request, response) -> {

            System.out.println("Wow" + Twitter.getTwitter().getUsers().size());

            String id = request.cookie("id");

            //Search for the user in the data structure
            if (!Twitter.getTwitter().existUser(id)) {
                response.status(404);
                return "User does not exist. Sign in if you want to get the tweets";
            }

            response.type("application/json");
            response.status(200);

            List<String> locations = new ArrayList<>(Arrays.asList(request.splat()[0].split("&")));
            List<String> tags = new ArrayList<>(Arrays.asList(request.splat()[1].split("&")));
            List<String> mentions = new ArrayList<>(Arrays.asList(request.splat()[2].split("&")));

            return new Gson().toJson(new TweetStub().findTweets(id, locations, mentions, tags));
            //TODO check error in filters
        });

        post("/tweets/subscription/*/*/*", (request, response) -> {

            String id = request.cookie("id");

            //Search for the user in the data structure
            if (!Twitter.getTwitter().existUser(id)) {
                response.status(404);
                return "User does not exist. Sign in if you want to get the tweets";
            }

            List<String> locations = new ArrayList<>(Arrays.asList(request.splat()[0].split("&")));
            List<String> tags = new ArrayList<>(Arrays.asList(request.splat()[1].split("&")));
            List<String> mentions = new ArrayList<>(Arrays.asList(request.splat()[2].split("&")));

            if (!new TweetStub().subscription(id, locations, tags, mentions)){
                response.type("application/json");
                response.status(404);
                return "WebSocket connection is closed";
            }
            response.type("application/json");
            response.status(200);

            return "Start streaming subscriptions";
            //TODO check error in filters
        });

    }
}
