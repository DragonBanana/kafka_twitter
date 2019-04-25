package kafka.model;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class TweetTest {

    private Tweet tweetLucaT;
    private Tweet tweetLucaF;
    private Tweet tweetDavide;

    @Before
    public void setUp() throws Exception {
        tweetLucaT = new Tweet("luca t", "ciao sono luca t", "now", "verona", new ArrayList<>(), new ArrayList<>());
        tweetLucaF = new Tweet("luca f", "ciao sono luca f", "now", "roma", new ArrayList<>(), Arrays.asList("@skrra"));
        tweetDavide = new Tweet("luca t", "ciao sono luca t", "now", "cina", Arrays.asList("#swag"), Arrays.asList("@skusku"));
    }

    @After
    public void tearDown() throws Exception {
    }

    /**
     * Asking to Kafka if it has the three topic (tag, mention and location)
     */
    @Test
    public void getFilters() {
        assertTrue(tweetLucaT.getFilters().contains(Topic.LOCATION));
        assertFalse(tweetLucaT.getFilters().contains(Topic.MENTION));
        assertFalse(tweetLucaT.getFilters().contains(Topic.TAG));
        assertTrue(tweetLucaF.getFilters().contains(Topic.LOCATION));
        assertTrue(tweetLucaF.getFilters().contains(Topic.MENTION));
        assertFalse(tweetLucaF.getFilters().contains(Topic.TAG));
        assertTrue(tweetDavide.getFilters().contains(Topic.LOCATION));
        assertTrue(tweetDavide.getFilters().contains(Topic.MENTION));
        assertTrue(tweetDavide.getFilters().contains(Topic.TAG));
    }

}
