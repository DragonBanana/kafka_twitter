package kafka.rest;

import org.slf4j.LoggerFactory;
import spark.servlet.SparkApplication;
import spark.servlet.SparkFilter;

import javax.ws.rs.*;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.sse.Sse;
import javax.ws.rs.sse.SseEventSink;

import static spark.Spark.*;

@Path("sample")
@ApplicationPath("sample")
public class TweetSSERoute extends Application implements SparkApplication {

    @GET
    @Path("prices")
    @Produces("text/event-stream")
    public void getStockPrices(@Context SseEventSink sseEventSink, @Context Sse sse) {
        //...
    }

    public void init() {
        path("/api", () -> {
            before("/*", (q, a) -> LoggerFactory.getLogger(TwitterRest.class).info("Received api call"));

            TweetRoute.configureRoutes();
            UserRoute.configureRoutes();
            SubscriptionRoute.configureRoutes();
        });
    }
}
