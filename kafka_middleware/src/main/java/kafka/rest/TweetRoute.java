package kafka.rest;

import com.google.gson.Gson;
import kafka.model.Tweet;
import kafka.model.Twitter;
import spark.Response;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
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

            List<String> locations = new ArrayList<>(Arrays.asList(request.splat()[0].split("&")));
            List<String> tags = new ArrayList<>(Arrays.asList(request.splat()[1].split("&")));
            List<String> mentions = new ArrayList<>(Arrays.asList(request.splat()[2].split("&")));

            return new Gson().toJson(new TweetStub().findTweets(id, locations, mentions, tags));
            //TODO check error in filters
        });

        post("/tweets/subscription/*/*/*",(request, response) -> {

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

        get("/sse",(request, response) -> {


            System.out.println("richiesta ricevuta");

            response.header("Connection", "keep-alive");
            response.header("Content-Type", "text/event-stream");
            response.header("Cache-control", "no-cache");
            response.raw().setContentType("text/event-stream");
            response.header("Access-Control-Allow-Origin", "*");
            response.status(200);
            //OutputStream out = null;
            //try {
                //out = response.raw().getOutputStream();
            //} catch (IOException e) {
            //    e.printStackTrace();
            //}

            response.raw().setStatus(200);
            response.raw().setContentType("text/event-stream");
            //response.raw().setHeader("Access-Control-Allow-Origin", "*");
            response.raw().setHeader("Connection", "keep-alive");
            response.raw().setHeader("Content-Type", "text/event-stream");
            response.raw().setHeader("Cache-control", "no-cache");
            PrintWriter p = response.raw().getWriter();
            p.print("id: 99\nevent: eventType\ndata: CIAO\n\n\n");
            p.flush();



            new Thread(() -> {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                //send(response);
            }).start();

            return "id: 99\nevent: eventType\ndata: RETURN \n\n\n";
        });
    }

    private static void send(Response response) {

        for(int i = 0; i < 1; i++) {
            OutputStream out = null;
            try {
                out = response.raw().getOutputStream();
            } catch (IOException e) {
                e.printStackTrace();
            }

            response.raw().setStatus(200);
            response.raw().setContentType("text/event-stream");
            //response.raw().setHeader("Access-Control-Allow-Origin", "*");
            response.raw().setHeader("Connection", "keep-alive");
            response.raw().setHeader("Content-Type", "text/event-stream");
            response.raw().setHeader("Cache-control", "no-cache");
            PrintWriter p = new PrintWriter(out);
            p.print("id: 99\nevent: eventType\ndata: CIAO\n\n\n");


        }
    }
}
