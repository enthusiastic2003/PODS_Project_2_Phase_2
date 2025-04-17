package com.example.Routers;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import com.example.Requests.OrderPostRequests;

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
import com.example.ImportantActors.OneOrder;
import com.example.OrderPutRequest;
import com.example.Responses.OrderDelete;
import com.example.Responses.OrderGetResponse;
import com.example.Responses.OrderPostResponse;
import com.example.Responses.OrderPutResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// To handle HTTP requests related to orders

public class OrderRoutes {

    private final static Logger log = LoggerFactory.getLogger(OrderRoutes.class);
    private final Duration askTimeout;
    private final Scheduler scheduler;
    // Instead of a map of order actors, we now hold a reference to the singleton Gateway actor.
    private final ActorRef<com.example.Gateway.Gateway.Command> gateway;

    public OrderRoutes(ActorSystem<?> system, ActorRef<Gateway.Command> gateway) {
        this.gateway = gateway;
        this.scheduler = system.scheduler();
        this.askTimeout = system.settings().config().getDuration("my-app.routes.ask-timeout");
    }

    public Route orderRoutes(){
        return pathPrefix("orders", () ->
            concat(
                    pathEnd(() ->
                            post(() ->
                            // 🔹 Handle POST request to create a new order
                                    entity(Jackson.unmarshaller(OrderPostRequests.class), orderRequest -> {
                                        log.info("Received POST /orders request");

                                        // Send message to Gateway to process the order
                                        CompletionStage<OrderPostResponse.Response> response =
                                                AskPattern.ask(
                                                        gateway,
                                                        replyTo -> new Gateway.PlaceOrder(orderRequest, replyTo),
                                                        askTimeout,
                                                        scheduler
                                                );

                                        return onSuccess(response, orderResponse -> {
                                            if (orderResponse instanceof OrderPostResponse.OrderSuccess) {
                                                return complete(StatusCodes.CREATED, orderResponse, Jackson.marshaller());
                                            } else if (orderResponse instanceof OrderPostResponse.OrderFailure) {
                                                OrderPostResponse.OrderFailure failureResponse = (OrderPostResponse.OrderFailure) orderResponse;
                                                return complete(failureResponse.statusCode, failureResponse.message, Jackson.marshaller());
                                            } else {
                                                return complete(StatusCodes.INTERNAL_SERVER_ERROR, "Unknown response", Jackson.marshaller());
                                            }
                                        });

                                    })
                            )
                    ),
                // Routes that require an orderId path parameter
                path(PathMatchers.integerSegment(), (Integer orderId) ->
                    concat(
                        // GET order details
                            get(() -> {
                                log.info("Received GET request for Order with ID: {}", orderId);

                                return rejectEmptyResponse(() ->
                                        onSuccess(
                                                AskPattern.<com.example.Gateway.Gateway.Command, OrderGetResponse.Response>ask(
                                                        gateway,
                                                        replyTo -> new Gateway.GetOrder(orderId, replyTo),
                                                        askTimeout,
                                                        scheduler
                                                ),
                                                response -> {
                                                    if (response instanceof OrderGetResponse.OrderSuccess success) {
                                                        return complete(StatusCodes.OK, success.order, Jackson.marshaller());  // 🔹 Return the order
                                                    } else {
                                                        return complete(StatusCodes.NOT_FOUND, "Order not found");
                                                    }
                                                }
                                        )
                                );
                            }),

                        // DELETE order
                        delete(() -> {
                            System.out.println("Http Delete request for "+orderId+" received by Routes");
                            CompletionStage<OrderDelete.Response> response = AskPattern.ask(
                                gateway,
                                replyTo -> new Gateway.DeleteOrder(orderId, replyTo),
                                askTimeout,
                                scheduler
                            );

                            return onSuccess(response, orderDeleteResponse -> {
                                if (orderDeleteResponse instanceof OrderDelete.Success) {
                                    return complete(StatusCodes.OK, ((OrderDelete.Success) orderDeleteResponse).message);
                                } else if (orderDeleteResponse instanceof OrderDelete.Failure) {
                                    return complete(StatusCodes.BAD_REQUEST, ((OrderDelete.Failure) orderDeleteResponse).message);
                                } else {
                                    return complete(StatusCodes.INTERNAL_SERVER_ERROR, "Unknown response");
                                }
                            });

                            // return complete(StatusCodes.OK, "DELETE Orders HTTP request received with orderId: "+orderId);
                        }),
                        // PUT order status update
                        put(() ->
                            entity(Jackson.unmarshaller(OrderPutRequest.class), orderPutRequest -> {
                                log.info("PUT Order HTTP request received for orderID: {} with new status: {}",
                                    orderPutRequest.order_id, orderPutRequest.status);
                                return onSuccess(
                                    AskPattern.<Gateway.Command, OrderPutResponse>ask(
                                        gateway,
                                        replyTo -> new Gateway.PutOrderStatus(orderPutRequest.order_id, orderPutRequest.status, replyTo),
                                        askTimeout,
                                        scheduler
                                    ),
                                    putResponse -> complete(StatusCodes.get(putResponse.status.intValue()), putResponse.message)
                                );
                            })
                        )
                    )
                ),
                // A simple example for users-related route under orders
                pathPrefix("users", () ->
                    path(PathMatchers.integerSegment(), (Integer userId) ->
                        get(() -> {
                            log.info("GET Orders Users HTTP request with user_idd: {}", userId);
                            return complete(StatusCodes.OK, "GET Orders Users HTTP request with user_idd " + userId);
                        })
                    )
                )
            )
        );
    }
}
