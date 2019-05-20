package kafka.model;


import com.google.gson.Gson;
import org.eclipse.jetty.websocket.api.Session;

import java.io.IOException;
import java.util.List;

public class VirtualClient {

    private Session session;

    public VirtualClient() {
        this.session = null;
    }

    public VirtualClient(Session session) {
        this.session = session;
    }

    void notityTweets(List<Tweet> tweets) {
        if(this.isConnected()) {
            tweets.forEach(t -> {
                try {
                    session.getRemote().sendString(new Gson().toJson(t));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    public boolean isConnected(){
        if(session != null) {
            return session.isOpen();
        }
        return false;
    }
}
