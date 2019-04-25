package kafka.utility;

import kafka.model.Tweet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class TweetFilterTest {

    private List<Tweet> tweetList;

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
    }

    @After
    public void tearDown() {
    }


    /**
     * Testing the filter for one location.
     */
    @Test
    public void filterByLocations() {
        assertEquals(1, TweetFilter.filterByLocations(tweetList, Collections.singletonList("loc1")).size());
        assertTrue(TweetFilter.filterByLocations(tweetList, Collections.singletonList("loc1")).get(0).equals(tweetList.get(0)));

        assertEquals(1, TweetFilter.filterByLocations(tweetList, Collections.singletonList("loc2")).size());
        assertTrue(TweetFilter.filterByLocations(tweetList, Collections.singletonList("loc2")).get(0).equals(tweetList.get(1)));

        assertEquals(1, TweetFilter.filterByLocations(tweetList, Collections.singletonList("loc3")).size());
        assertTrue(TweetFilter.filterByLocations(tweetList, Collections.singletonList("loc3")).get(0).equals(tweetList.get(2)));
    }

    /**
     * Testing the filter for multiple location.
     */
    @Test
    public void filterByMultipleLocations() {

        List<String> filter1 = new ArrayList<>();
        filter1.add("loc1");
        filter1.add("loc2");
        filter1.add("loc3");

        assertEquals(3, TweetFilter.filterByLocations(tweetList, filter1).size());
        assertTrue(TweetFilter.filterByLocations(tweetList, filter1).stream().anyMatch(t -> t.equals(tweetList.get(0))));
        assertTrue(TweetFilter.filterByLocations(tweetList, filter1).stream().anyMatch(t -> t.equals(tweetList.get(1))));
        assertTrue(TweetFilter.filterByLocations(tweetList, filter1).stream().anyMatch(t -> t.equals(tweetList.get(2))));

        List<String> filter2 = new ArrayList<>();
        filter2.add("loc1");
        filter2.add("loc2");

        assertEquals(2, TweetFilter.filterByLocations(tweetList, filter2).size());
        assertTrue(TweetFilter.filterByLocations(tweetList, filter2).stream().anyMatch(t -> t.equals(tweetList.get(0))));
        assertTrue(TweetFilter.filterByLocations(tweetList, filter2).stream().anyMatch(t -> t.equals(tweetList.get(1))));
    }

    /**
     * Testing the filter for one tag.
     */
    @Test
    public void filterByTags() {

        assertEquals(1, TweetFilter.filterByTags(tweetList, Collections.singletonList("#tag1")).size());
        assertTrue(TweetFilter.filterByTags(tweetList, Collections.singletonList("#tag1")).get(0).equals(tweetList.get(0)));

        assertEquals(2, TweetFilter.filterByTags(tweetList, Collections.singletonList("#tag2")).size());
        assertTrue(TweetFilter.filterByTags(tweetList, Collections.singletonList("#tag2")).stream().anyMatch(t -> t.equals(tweetList.get(0))));
        assertTrue(TweetFilter.filterByTags(tweetList, Collections.singletonList("#tag2")).stream().anyMatch(t -> t.equals(tweetList.get(1))));
        assertTrue(TweetFilter.filterByTags(tweetList, Collections.singletonList("#tag2")).stream().noneMatch(t -> t.equals(tweetList.get(2))));

        assertEquals(3, TweetFilter.filterByTags(tweetList, Collections.singletonList("#tag3")).size());
        assertTrue(TweetFilter.filterByTags(tweetList, Collections.singletonList("#tag3")).stream().anyMatch(t -> t.equals(tweetList.get(0))));
        assertTrue(TweetFilter.filterByTags(tweetList, Collections.singletonList("#tag3")).stream().anyMatch(t -> t.equals(tweetList.get(1))));
        assertTrue(TweetFilter.filterByTags(tweetList, Collections.singletonList("#tag3")).stream().anyMatch(t -> t.equals(tweetList.get(2))));

        assertEquals(2, TweetFilter.filterByTags(tweetList, Collections.singletonList("#tag4")).size());
        assertTrue(TweetFilter.filterByTags(tweetList, Collections.singletonList("#tag4")).stream().noneMatch(t -> t.equals(tweetList.get(0))));
        assertTrue(TweetFilter.filterByTags(tweetList, Collections.singletonList("#tag4")).stream().anyMatch(t -> t.equals(tweetList.get(1))));
        assertTrue(TweetFilter.filterByTags(tweetList, Collections.singletonList("#tag4")).stream().anyMatch(t -> t.equals(tweetList.get(2))));

        assertEquals(1, TweetFilter.filterByTags(tweetList, Collections.singletonList("#tag5")).size());
        assertTrue(TweetFilter.filterByTags(tweetList, Collections.singletonList("#tag5")).get(0).equals(tweetList.get(2)));
    }

    @Test
    public void filterByMentions() {

        assertEquals(1, TweetFilter.filterByMentions(tweetList, Collections.singletonList("@men1")).size());
        assertTrue(TweetFilter.filterByMentions(tweetList, Collections.singletonList("@men1")).get(0).equals(tweetList.get(0)));

        assertEquals(2, TweetFilter.filterByMentions(tweetList, Collections.singletonList("@men2")).size());
        assertTrue(TweetFilter.filterByMentions(tweetList, Collections.singletonList("@men2")).stream().anyMatch(t -> t.equals(tweetList.get(0))));
        assertTrue(TweetFilter.filterByMentions(tweetList, Collections.singletonList("@men2")).stream().anyMatch(t -> t.equals(tweetList.get(1))));
        assertTrue(TweetFilter.filterByMentions(tweetList, Collections.singletonList("@men2")).stream().noneMatch(t -> t.equals(tweetList.get(2))));

        assertEquals(3, TweetFilter.filterByMentions(tweetList, Collections.singletonList("@men3")).size());
        assertTrue(TweetFilter.filterByMentions(tweetList, Collections.singletonList("@men3")).stream().anyMatch(t -> t.equals(tweetList.get(0))));
        assertTrue(TweetFilter.filterByMentions(tweetList, Collections.singletonList("@men3")).stream().anyMatch(t -> t.equals(tweetList.get(1))));
        assertTrue(TweetFilter.filterByMentions(tweetList, Collections.singletonList("@men3")).stream().anyMatch(t -> t.equals(tweetList.get(2))));

        assertEquals(2, TweetFilter.filterByMentions(tweetList, Collections.singletonList("@men4")).size());
        assertTrue(TweetFilter.filterByMentions(tweetList, Collections.singletonList("@men4")).stream().noneMatch(t -> t.equals(tweetList.get(0))));
        assertTrue(TweetFilter.filterByMentions(tweetList, Collections.singletonList("@men4")).stream().anyMatch(t -> t.equals(tweetList.get(1))));
        assertTrue(TweetFilter.filterByMentions(tweetList, Collections.singletonList("@men4")).stream().anyMatch(t -> t.equals(tweetList.get(2))));

        assertEquals(1, TweetFilter.filterByMentions(tweetList, Collections.singletonList("@men5")).size());
        assertTrue(TweetFilter.filterByMentions(tweetList, Collections.singletonList("@men5")).get(0).equals(tweetList.get(2)));
    }
}