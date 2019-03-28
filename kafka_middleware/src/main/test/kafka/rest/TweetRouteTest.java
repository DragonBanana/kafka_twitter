package kafka.rest;

import com.google.gson.Gson;
import kafka.model.Tweet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.util.ArrayList;

import static junit.framework.TestCase.assertTrue;
import static spark.Spark.before;
import static spark.Spark.path;

public class TweetRouteTest {

    private String POST_URL = "http://localhost:4567/tweets";
    private Thread server;

    @Before
    public void setUp() throws Exception {
        /*
        server = new Thread(() -> {
            path("/api", () -> {
                before("/*", (q, a) -> LoggerFactory.getLogger(TwitterRest.class).info("Received api call"));

                TweetRoute.configureRoutes();
                UserRoute.configureRoutes();
                SubscriptionRoute.configureRoutes();
            });
        });
        Thread.sleep(4000);*/
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }

    /**
     * Testing the returned value of the post method
     */
    @Test
    public void post1() {
        /*
        URL obj = null;
        Tweet t1 = new Tweet("luca t", "ciao sono luca t", "now", "verona", new ArrayList<>(), new ArrayList<>());
        try {
            obj = new URL(POST_URL);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod("POST");
            String urlParameters = new Gson().toJson(t1);
            // Send post request
            con.setDoOutput(true);
            DataOutputStream wr = new DataOutputStream(con.getOutputStream());
            wr.writeBytes(urlParameters);
            wr.flush();
            wr.close();
            int responseCode = con.getResponseCode();
            System.out.println(responseCode);
            assertTrue(responseCode >= 200 && responseCode < 300);
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            Tweet t2 = new Gson().fromJson(response.toString(), Tweet.class);
            assertTrue(t1.equals(t2));
        } catch (Exception e) {
            e.printStackTrace();
        }

         */
    }


}
