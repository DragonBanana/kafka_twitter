package kafka.rest;

import com.google.gson.Gson;
import kafka.model.Tweet;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class TweetStubTest {

    @Test
    public void save() {
        TweetStub tweetStub = new TweetStub();
        List<String> tags = new ArrayList<>();
        tags.add("#swag");
        List<String> tags1 = new ArrayList<>();
        tags1.add("#swag");
        tags1.add("#swag2");
        List<String> mentions = new ArrayList<>();
        mentions.add("@bellofigo");
        //tweetStub.save(new Tweet("luca", "Hello from the stub", "now","verona", tags, mentions ));
        tweetStub.save(new Tweet("luca", "Hello from the stub", "now","verona", tags, mentions ));
        //tweetStub.save(new Tweet("luca", "Hello from the stub", "now","verona", tags1, mentions ));
        tweetStub.findLatestByTag("luca", Arrays.asList("#swag"), "nofilters").stream().forEach(t -> System.out.println(new Gson().toJson(t)));
    }
}