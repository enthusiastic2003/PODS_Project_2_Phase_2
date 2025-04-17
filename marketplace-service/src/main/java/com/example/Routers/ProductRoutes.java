package com.example.Routers;

import java.time.Duration;

import com.example.Responses.ProductFound;
import com.example.Responses.ProductResponse;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Scheduler;
import akka.actor.typed.javadsl.AskPattern;
import akka.http.javadsl.marshallers.jackson.Jackson;
import static akka.http.javadsl.server.Directives.*;

import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.server.PathMatchers;
import akka.http.javadsl.server.Route;
import com.example.Gateway.Gateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// To handle HTTP requests related to products
public class ProductRoutes {

  private final static Logger log = LoggerFactory.getLogger(ProductRoutes.class);
  // Now we hold a reference to the Gateway actor rather than a product map.
  private final ActorRef<Gateway.Command> gateway;
  private final Duration askTimeout;
  private final Scheduler scheduler;
  private final ActorSystem<?> system;

  public ProductRoutes(ActorSystem<?> system, ActorRef<Gateway.Command> gateway) {
    this.system = system;
    this.gateway = gateway;
    this.scheduler = system.scheduler();
    this.askTimeout = system.settings().config().getDuration("my-app.routes.ask-timeout");
  }

  public Route productRoutes() {
    return pathPrefix("products", () ->
        concat(
            path(PathMatchers.integerSegment(), (Integer id) ->
                concat(
                    get(() -> {
                        // 🔹 Handle GET request to fetch product details
                        log.info("Received GET request for product with ID: {}", id);
                        // Ask the gateway actor for product details
                        return onSuccess(
                                AskPattern.<Gateway.Command, ProductResponse>ask(
                                        gateway,
                                        replyTo -> new Gateway.GetProduct(id, replyTo),
                                        askTimeout,
                                        scheduler
                                ),
                                productResponse -> {
                                    if (productResponse instanceof ProductFound) {
                                        return complete(StatusCodes.OK, ((ProductFound) productResponse).product, Jackson.marshaller());
                                    } else {
                                        log.warn("Product not found with ID: {}", id);
                                        return complete(StatusCodes.NOT_FOUND, "Product not found");
                                    }
                                }
                        );
                    })
                )
            )
        )
    );
  }
}
