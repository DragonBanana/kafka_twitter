package kafka;

import kafka.model.Tweet;
import kafka.rest.TweetRoute;

import java.util.Arrays;
import java.util.Date;

public class App {

    public static void main(String[] args) throws Exception {
        Tweet tweet = new Tweet("author", "content", Long.toString(new Date().getSeconds()), "s", Arrays.asList("tags"), Arrays.asList("mention"));
        TweetRoute.configureRoutes();
    }


}
