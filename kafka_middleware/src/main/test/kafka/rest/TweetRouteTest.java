package kafka.rest;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.restassured.http.ContentType;
import kafka.model.Tweet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.restassured.RestAssured.given;
import static io.restassured.RestAssured.when;
import static org.junit.Assert.assertTrue;
import static spark.Spark.*;

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
        Thread.sleep(1000);
    }

    @After
    public void tearDown() throws Exception {
        stop();
        server.stop();
        Thread.sleep(1000);
    }


    @Test
    public void create_a_user_successful_and_unsuccessful() {
        when().
                post("http://localhost:4567/api/users/5").
                then().
                statusCode(200);


        when().
                post("http://localhost:4567/api/users/5").
                then().
                statusCode(200);
    }

    @Test
    public void post_tweet_successful() {
        when().
                post("http://localhost:4567/api/users/5").

                then().
                contentType(ContentType.JSON);

        Tweet tweetLucaT = new Tweet("luca t", "ciao sono luca t", "now", "verona", new ArrayList<>(), new ArrayList<>());
        //Tweet tweetLucaF = new Tweet("luca f", "ciao sono luca f", "now", "roma", new ArrayList<>(), Arrays.asList("@skrra"));
        Tweet tweetDavide = new Tweet("davide", "ciao sono davide", "now", "cina", Arrays.asList("#swag"), Arrays.asList("@skusku"));

        List<Tweet> tweets = new ArrayList<>();
        //tweets.add(tweetLucaF);
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

    @Test
    public void post_not_tweet_unsuccessful() {
        when().
                post("http://localhost:4567/api/users/5");


        given().
                port(4567).
                cookie("id", "5").
                body("{\n" +
                        "\t\"Author\":\"Luco\",\n" +
                        "\t\"content\":\"Hello Twitter\",\n" +
                        "\t\"tags\" : [\"swag\"],\n" +
                        "\t\"mentions\" : [\"Luco\"]\n" +
                        "\t\n" +
                        "}").
                when().
                post("/api/tweets").

                then().
                contentType(ContentType.JSON).
                statusCode(400);
    }

    @Test
    public void post_tweet_unsuccessful() {
        when().
                post("http://localhost:4567/api/users/5");


        given().
                port(4567).
                cookie("id", "5").
                body("{\n" +
                        "\t\"A\":\"Luco\",\n" +
                        "\t\"C\":\"Hello Twitter\",\n" +
                        "\t\"T\" : \"17.30\",\n" +
                        "\t\"L\" : \"Milan\",\n" +
                        "\t\"T\" : [swag,fun],\n" +
                        "\t\"M\" : [@Luco]\n" +
                        "\t\n" +
                        "}").
                when().
                post("/api/tweets").

                then().
                contentType(ContentType.JSON).
                statusCode(400);
    }

    @Test
    public void get_tweet_successful() throws InterruptedException {
        String id = "5";
        when().
                post("http://localhost:4567/api/users/" + id).

                then().
                contentType(ContentType.JSON);

        Tweet tweetLucaT = new Tweet("luca t", "ciao sono luca t", "now", "verona", new ArrayList<>(), new ArrayList<>());
        Tweet tweetLucaF = new Tweet("luca f", "ciao sono luca f", "now", "roma", new ArrayList<>(), Arrays.asList("@skrra"));
        Tweet tweetDavide = new Tweet("davide", "ciao sono davide", "now", "cina", Arrays.asList("#swag"), Arrays.asList("@skusku"));

        List<Tweet> tweets = new ArrayList<>();
        tweets.add(tweetLucaF);
        tweets.add(tweetLucaT);
        tweets.add(tweetDavide);

        tweets.forEach(t -> {

            String response = given().
                    port(4567).
                    cookie("id", id).
                    body(new Gson().toJson(t)).
                    when().
                    post("/api/tweets").

                    then().
                    contentType(ContentType.JSON).
                    statusCode(200).
                    extract().
                    response().asString();
        });

        Thread.sleep(5000);
        tweets.forEach(t -> {

            String response = given().
                    port(4567).
                    cookie("id", id).
                    when().
                    get("/api/tweets/" + t.getLocation() + "/all/all/latest").

                    then().
                    contentType(ContentType.JSON).
                    statusCode(200).
                    extract().
                    response().asString();

            Type listType = new TypeToken<ArrayList<Tweet>>(){}.getType();
            List<Tweet> tweetList = new Gson().fromJson(response, listType);
            assertTrue(tweetList.stream().anyMatch(tw -> tw.equals(t)));
        });
    }

}
