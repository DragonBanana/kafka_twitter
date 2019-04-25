package kafka.rest;

import com.google.gson.Gson;
import kafka.model.Tweet;
import kafka.model.Twitter;
import kafka.utility.TweetValidator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static spark.Spark.get;
import static spark.Spark.post;

public class TweetRoute {

    public static void configureRoutes() {


        post("/tweets", (request, response) -> {
            //response.header("Access-Control-Allow-Origin", "http://"+"127.0.0.1"+":"+"4567");
            //response.header("Access-Control-Allow-Origin", "http://"+"127.0.0.1"+":"+"4567" + "/*");
            response.header("Access-Control-Allow-Credentials", "true");
            response.type("application/json");
            String id = request.cookie("id");
            //Search for the user in the data structure
            if (!Twitter.getTwitter().existUser(id)) {
                response.status(400);
                return "{\"type\" : \"error\", \"message\" : \"user does not exist, sign in if you want to post a tweet\"}";
            }

            System.out.println(request.body());
            Tweet tweet = new Gson().fromJson(request.body(), Tweet.class);
            if (TweetValidator.isValid(tweet)) {
                response.status(200);
                TweetValidator.toStandardTweet(tweet);
                new TweetStub().save(tweet);
                return new Gson().toJson(tweet);
            }else{
                response.status(400);
                return "{\"type\" : \"error\", \"message\" : \"tweet is not well formatted\"}";
            }

        });

        get("/tweets/*/*/*/latest", (request, response) -> {

            //response.header("Access-Control-Allow-Origin", "http://"+"127.0.0.1"+":"+"4567" + "/*");
            //response.header("Access-Control-Allow-Origin", "*");
            response.header("Access-Control-Allow-Credentials", "true");

            String id = request.cookie("id");
            response.type("application/json");

            //Search for the user in the data structure
            if (!Twitter.getTwitter().existUser(id)) {
                response.status(400);
                return "{\"type\" : \"error\", \"message\" : \"user does not exist with id "+id+", sign in if you want to post a tweet\"}";
            }

            response.status(200);

            List<String> locations = new ArrayList<>(Arrays.asList(request.splat()[0].split("&")));
            List<String> tags = new ArrayList<>(Arrays.asList(request.splat()[1].split("&")));
            List<String> mentions = new ArrayList<>(Arrays.asList(request.splat()[2].split("&")));

            return new Gson().toJson(new TweetStub().findTweets(id, locations, mentions, tags));
            //TODO check error in filters
        });

        post("/tweets/subscription/*/*/*", (request, response) -> {

            String id = request.cookie("id");
            response.header("Access-Control-Allow-Origin", "http://"+"127.0.0.1"+":"+"4567" + "/*");

            //Search for the user in the data structure
            if (!Twitter.getTwitter().existUser(id)) {
                response.status(400);
                return "{\"type\" : \"error\", \"message\" : \"user does not exist, sign in if you want to post a tweet\"}";
            }

            List<String> locations = new ArrayList<>(Arrays.asList(request.splat()[0].split("&")));
            List<String> tags = new ArrayList<>(Arrays.asList(request.splat()[1].split("&")));
            List<String> mentions = new ArrayList<>(Arrays.asList(request.splat()[2].split("&")));

            if (!new TweetStub().subscription(id, locations, tags, mentions)){
                response.type("application/json");
                response.status(400);
                return "WebSocket connection is closed";
            }
            response.type("application/json");
            response.status(200);

            return "Start streaming subscriptions";
            //TODO check error in filters
        });

    }
}
