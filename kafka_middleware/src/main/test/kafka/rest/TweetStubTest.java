package kafka.rest;

import com.google.gson.Gson;
import kafka.model.Tweet;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static junit.framework.TestCase.assertTrue;

public class TweetStubTest {

    private List<Tweet> tweetList;
    private String id;
    private String filter;

    @Before
    public void setUp() {
        tweetList = new ArrayList<>();
        List<String> tags1 = new ArrayList<>();
        tags1.add("#tag1");
        tags1.add("#tag2");
        tags1.add("#tag3");
        List<String> mentions1 = new ArrayList<>();
        mentions1.add("@men1");
        mentions1.add("@men2");
        mentions1.add("@men3");
        tweetList.add(new Tweet("author1",
                "content1",
                "ts1",
                "loc1",
                tags1,
                mentions1));
        List<String> tags2 = new ArrayList<>();
        tags2.add("#tag2");
        tags2.add("#tag3");
        tags2.add("#tag4");
        List<String> mentions2 = new ArrayList<>();
        mentions2.add("@men2");
        mentions2.add("@men3");
        mentions2.add("@men4");
        tweetList.add(new Tweet("author2",
                "content2",
                "ts2",
                "loc2",
                tags2,
                mentions2));
        List<String> tags3 = new ArrayList<>();
        tags3.add("#tag3");
        tags3.add("#tag4");
        tags3.add("#tag5");
        List<String> mentions3 = new ArrayList<>();
        mentions3.add("@men3");
        mentions3.add("@men4");
        mentions3.add("@men5");
        tweetList.add(new Tweet("author3",
                "content3",
                "ts3",
                "loc3",
                tags3,
                mentions3));
        id = "test_id";
        filter = "test_filter";
    }

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
        try {
            tweetStub.save(new Tweet("luca", "Hello from the stub", "now","verona", tags, mentions ));
        } catch (Exception e) {
            e.printStackTrace();
        }
        //tweetStub.save(new Tweet("luca", "Hello from the stub", "now","verona", tags1, mentions ));
        tweetStub.findLatestByTags("luca", Collections.singletonList("#swag"), "nofilters").forEach(t -> System.out.println(new Gson().toJson(t)));
        tweetStub.findLatestByLocations("luca", Collections.singletonList("verona"), "nofilters").forEach(t -> System.out.println(new Gson().toJson(t)));
        //System.out.println(tweetStub.findLatestByLocation("luca", "verona", "nofilters").size());
    }

    @Test
    public void consumeTweetByLocation() {
        tweetList.forEach(t -> {
            try {
                new TweetStub().save(t);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        List<Tweet> tweets = new TweetStub().findLatestByLocations(id, Collections.singletonList("loc1"), filter);
        assertTrue(tweets.size() > 0);
        assertTrue(tweets.get(0).equals(tweetList.get(0)));

        tweets = new TweetStub().findLatestByLocations(id, Collections.singletonList("loc2"), filter);
        assertTrue(tweets.size() > 0);
        assertTrue(tweets.get(0).equals(tweetList.get(1)));

        tweets = new TweetStub().findLatestByLocations(id, Collections.singletonList("loc3"), filter);
        assertTrue(tweets.size() > 0);
        assertTrue(tweets.get(0).equals(tweetList.get(2)));
    }

    @Test
    public void consumeTweetByTag() {
        tweetList.forEach(t -> {
            try {
                new TweetStub().save(t);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        List<Tweet> tweets = new TweetStub().findLatestByTags(id, Collections.singletonList("#tag1"), filter+"#tag1");
        assertTrue(tweets.size() > 0);
        assertTrue(tweets.stream().anyMatch(t -> t.equals(tweetList.get(0))));

        tweets = new TweetStub().findLatestByTags(id, Collections.singletonList("#tag2"), filter+"#tag2");
        assertTrue(tweets.size() > 1);
        assertTrue(tweets.stream().anyMatch(t -> t.equals(tweetList.get(0))));
        assertTrue(tweets.stream().anyMatch(t -> t.equals(tweetList.get(1))));
        assertTrue(tweets.stream().noneMatch(t -> t.equals(tweetList.get(2))));


        tweets = new TweetStub().findLatestByTags(id, Collections.singletonList("#tag3"), filter+"#tag3");
        assertTrue(tweets.size() > 2);
        assertTrue(tweets.stream().anyMatch(t -> t.equals(tweetList.get(0))));
        assertTrue(tweets.stream().anyMatch(t -> t.equals(tweetList.get(1))));
        assertTrue(tweets.stream().anyMatch(t -> t.equals(tweetList.get(2))));

        tweets = new TweetStub().findLatestByTags(id, Collections.singletonList("#tag4"), filter+"#tag4");
        assertTrue(tweets.size() > 1);
        assertTrue(tweets.stream().noneMatch(t -> t.equals(tweetList.get(0))));
        assertTrue(tweets.stream().anyMatch(t -> t.equals(tweetList.get(1))));
        assertTrue(tweets.stream().anyMatch(t -> t.equals(tweetList.get(2))));
    }

    @Test
    public void consumeTweetByMention() {
        tweetList.forEach(t -> {
            try {
                new TweetStub().save(t);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        List<Tweet> tweets = new TweetStub().findLatestByMentions(id, Collections.singletonList("@men1"), filter+"@men1");
        assertTrue(tweets.size() > 0);
        assertTrue(tweets.stream().anyMatch(t -> t.equals(tweetList.get(0))));

        tweets = new TweetStub().findLatestByMentions(id, Collections.singletonList("@men2"), filter+"@men2");
        assertTrue(tweets.size() > 1);
        assertTrue(tweets.stream().anyMatch(t -> t.equals(tweetList.get(0))));
        assertTrue(tweets.stream().anyMatch(t -> t.equals(tweetList.get(1))));
        assertTrue(tweets.stream().noneMatch(t -> t.equals(tweetList.get(2))));


        tweets = new TweetStub().findLatestByMentions(id, Collections.singletonList("@men3"), filter+"@men3");
        assertTrue(tweets.size() > 2);
        assertTrue(tweets.stream().anyMatch(t -> t.equals(tweetList.get(0))));
        assertTrue(tweets.stream().anyMatch(t -> t.equals(tweetList.get(1))));
        assertTrue(tweets.stream().anyMatch(t -> t.equals(tweetList.get(2))));

        tweets = new TweetStub().findLatestByMentions(id, Collections.singletonList("@men4"), filter+"@men4");
        assertTrue(tweets.size() > 1);
        assertTrue(tweets.stream().noneMatch(t -> t.equals(tweetList.get(0))));
        assertTrue(tweets.stream().anyMatch(t -> t.equals(tweetList.get(1))));
        assertTrue(tweets.stream().anyMatch(t -> t.equals(tweetList.get(2))));
    }


}