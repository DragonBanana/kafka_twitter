package kafka.rest;

import com.google.gson.Gson;

import static spark.Spark.*;

public class TwitterRoute {

    public static void configureRoutes() {
        path("/api/contacts", () -> {
            get("", (request, response) -> service.findAll(), new Gson()::toJson);
            get("/:id", (request, response) -> service.findById(request), new Gson()::toJson);
            post("", (request, response) -> service.save(request, gson), new Gson()::toJson);
            put("", (request, response) -> service.update(request, gson), new Gson()::toJson);
            delete("", (request, response) -> service.delete(request), new Gson()::toJson);
        });
    }
}
