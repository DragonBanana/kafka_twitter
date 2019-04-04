package kafka.rest;

import com.google.gson.Gson;
import io.restassured.http.ContentType;
import kafka.model.Tweet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.restassured.RestAssured.given;
import static io.restassured.RestAssured.when;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;
import static spark.Spark.*;
import static spark.Spark.init;

public class TweetRouteTest {

    private Thread server;

    @Before
    public void setUp() throws InterruptedException {
        server = new Thread(() -> {

            path("/api", () -> {
                TweetRoute.configureRoutes();
                UserRoute.configureRoutes();
                SubscriptionRoute.configureRoutes();
            });
        });
        server.start();
        init();
        Thread.sleep(500);
    }

    @After
    public void tearDown() throws Exception {
        stop();
        server.stop();
        Thread.sleep(500);
    }


    @Test
    public void create_a_user_successful() {
        when().
                post("http://localhost:4567/api/users/5").
                then().
                statusCode(200).
                body(equalTo("New user created with id: 5"));


        when().
                post("http://localhost:4567/api/users/5").
                then().
                statusCode(200).
                body(equalTo("User already registered with id: 5"));
    }

    @Test
    public void post_tweet() {
        when().
                post("http://localhost:4567/api/users/5");

        Tweet tweetLucaT = new Tweet("luca t", "ciao sono luca t", "now", "verona", new ArrayList<>(), new ArrayList<>());
        Tweet tweetLucaF = new Tweet("luca f", "ciao sono luca f", "now", "roma", new ArrayList<>(), Arrays.asList("@skrra"));
        Tweet tweetDavide = new Tweet("luca t", "ciao sono luca t", "now", "cina", Arrays.asList("#swag"), Arrays.asList("@skusku"));

        List<Tweet> tweets = new ArrayList<>();
        tweets.add(tweetLucaF);
        tweets.add(tweetLucaT);
        tweets.add(tweetDavide);

        tweets.forEach(t -> {

            String response = given().
                    port(4567).
                    cookie("id", "5").
                    body(new Gson().toJson(t)).
                    when().
                    post("/api/tweets").

                    then().
                    contentType(ContentType.JSON).
                    statusCode(200).
                    extract().
                    response().asString();

            assertTrue(new Gson().fromJson(response, Tweet.class).equals(t));
        });

    }

}
