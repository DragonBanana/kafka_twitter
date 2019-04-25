package kafka.rest;

import kafka.model.Twitter;
import kafka.model.VirtualClient;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

import java.util.HashMap;
import java.util.Map;

@WebSocket
public class WSHandler {

    private static final Map<Session, String> map = new HashMap<>();

    public WSHandler() {
    }

    @OnWebSocketConnect
    public void onConnect(Session user) {
        String id = user.getUpgradeRequest().getCookies().stream().filter(s -> s.getName().equals("id")).findAny().get().getValue();
        System.out.println(id);
        Twitter.getTwitter().getUser(id).setVirtualClient(new VirtualClient(user));
        map.put(user, id);
    }

    @OnWebSocketMessage
    public void onMessage(Session user, String message) {
        System.out.println(user.getUpgradeRequest().getCookies());
        if (user != null && user.isOpen() && message != null) {
            System.out.println("websocket received message: " + message);
            if (message.startsWith("id")) {
                String id = message.split(" ")[1];
                if (Twitter.getTwitter().existUser(id)) {
                    Twitter.getTwitter().getUser(id).setVirtualClient(new VirtualClient(user));
                    map.put(user, id);
                }
            } else if (map.containsKey(user)) {
                if(message.startsWith("location")) {
                    String id = map.get(user);
                    String location = message.split(" ")[1];
                    Twitter.getTwitter().getUser(id).getSubscriptionStub().followLocation(location);
                } else if (message.startsWith("tag")) {
                    String id = map.get(user);
                    String tag = message.split(" ")[1];
                    Twitter.getTwitter().getUser(id).getSubscriptionStub().followTag(tag);
                } else if (message.startsWith("mention")) {
                    String id = map.get(user);
                    String mention = message.split(" ")[1];
                    Twitter.getTwitter().getUser(id).getSubscriptionStub().followUser(mention);
                }
            }
        }
    }
}
