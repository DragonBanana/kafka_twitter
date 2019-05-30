package kafka.rest;

import kafka.model.Twitter;
import kafka.model.User;
import kafka.model.VirtualClient;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class WebSocketRoutineTest {

    @Test
    public void run1() {
        long timestamp = 400000;
        TweetStub tweetStub = Mockito.mock(TweetStub.class);
        Mockito.when(tweetStub.findTweets("luca", null,null,null)).thenReturn(new ArrayList<>());
        Mockito.when(tweetStub.findTweets("davide", null,null,null)).thenReturn(new ArrayList<>());

        WebSocketRoutine webSocketRoutine = new WebSocketRoutine(timestamp, tweetStub);

        Twitter twitter = Twitter.getTwitter();

        twitter.createNewUser("luca");
        twitter.createNewUser("davide");


        User luca = twitter.getUser("luca");
        User davide = twitter.getUser("davide");

        luca.setVirtualClient(new VirtualClient());
        davide.setVirtualClient(new VirtualClient());


        webSocketRoutine.run();

        assertEquals( timestamp, (long) luca.getSubscriptionStub().lastPoll());
        assertEquals( timestamp, (long) davide.getSubscriptionStub().lastPoll());
    }

    @Test
    public void run2() {
        long timestamp = 1000;
        TweetStub tweetStub = Mockito.mock(TweetStub.class);
        Mockito.when(tweetStub.findTweets("luca", null,null,null)).thenReturn(new ArrayList<>());
        Mockito.when(tweetStub.findTweets("davide", null,null,null)).thenReturn(new ArrayList<>());

        WebSocketRoutine webSocketRoutine = new WebSocketRoutine(timestamp, tweetStub);

        Twitter twitter = Twitter.getTwitter();

        twitter.createNewUser("luca");
        twitter.createNewUser("davide");


        User luca = twitter.getUser("luca");
        User davide = twitter.getUser("davide");

        luca.setVirtualClient(new VirtualClient());
        davide.setVirtualClient(new VirtualClient());

        webSocketRoutine.run();


        assertNotEquals( timestamp, (long) luca.getSubscriptionStub().lastPoll());
        assertNotEquals( timestamp, (long) davide.getSubscriptionStub().lastPoll());


    }

    @Test
    public void run3() {
        long timestamp = 400000;
        TweetStub tweetStub = Mockito.mock(TweetStub.class);
        Mockito.when(tweetStub.findTweets("luca", null,null,null)).thenReturn(new ArrayList<>());
        Mockito.when(tweetStub.findTweets("davide", null,null,null)).thenReturn(new ArrayList<>());
        Mockito.when(tweetStub.findTweets("alessio", null,null,null)).thenReturn(new ArrayList<>());

        WebSocketRoutine webSocketRoutine = new WebSocketRoutine(timestamp, tweetStub);

        Twitter twitter = Twitter.getTwitter();

        twitter.createNewUser("alessio");
        twitter.createNewUser("luca");
        twitter.createNewUser("davide");


        User alessio = twitter.getUser("alessio");
        User luca = twitter.getUser("luca");
        User davide = twitter.getUser("davide");

        luca.setVirtualClient(new VirtualClient());
        davide.setVirtualClient(new VirtualClient());
        alessio.setVirtualClient(new VirtualClient());

        alessio.getSubscriptionStub().updatePoll((long) 399999);

        webSocketRoutine.run();

        assertEquals( timestamp, (long) luca.getSubscriptionStub().lastPoll());
        assertEquals( timestamp, (long) davide.getSubscriptionStub().lastPoll());
        assertEquals( 399999, (long) alessio.getSubscriptionStub().lastPoll() );
        assertNotEquals( timestamp, (long) alessio.getSubscriptionStub().lastPoll());


    }

}