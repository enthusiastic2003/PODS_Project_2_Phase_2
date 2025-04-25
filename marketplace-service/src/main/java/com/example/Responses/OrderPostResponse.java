package com.example.Responses;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.http.javadsl.model.StatusCode;
import com.example.ImportantActors.OrderItem;
import com.example.ImportantActors.OrderStatus;
import com.example.SerializableTraitClass;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

// OrderPostResponse Actor
public class OrderPostResponse extends AbstractBehavior<OrderPostResponse.Response> {

    // Interface for all message types
    public interface Response {}

    // Message to indicate order success with details
    public static class OrderSuccess extends SerializableTraitClass implements Response {
        @JsonProperty("order_id")
        public final int order_id;

        @JsonProperty("user_id")
        public final int user_id;

        @JsonProperty("total_price")
        public final int total_price;

        @JsonProperty("status")
        public final OrderStatus status;

        @JsonProperty("items")
        public final List<OrderItem> items;

        // Constructor for Jackson deserialization
        @JsonCreator
        public OrderSuccess(
                @JsonProperty("order_id") int orderId,
                @JsonProperty("user_id") int userId,
                @JsonProperty("total_price") int totalPrice,
                @JsonProperty("status") OrderStatus status,
                @JsonProperty("items") List<OrderItem> items
        ) {
            this.order_id = orderId;
            this.user_id = userId;
            this.total_price = totalPrice;
            this.status = status;
            this.items = items;
        }
    }

    // Message to indicate order failure with details
    public static class OrderFailure extends SerializableTraitClass implements Response {
        @JsonProperty("status_code")
        public final int statusCode;

        @JsonProperty("message")
        public final String message;

        // Constructor for Jackson deserialization
        @JsonCreator
        public OrderFailure(
                @JsonProperty("status_code") int statusCode,
                @JsonProperty("message") String message
        ) {
            this.statusCode = statusCode;
            this.message = message;
        }
    }

    // Constructor for the actor
    public OrderPostResponse(ActorContext<Response> context) {
        super(context);
    }

    // Factory method to create the actor instance
    public static Behavior<Response> create() {
        return Behaviors.setup(OrderPostResponse::new);
    }

    @Override
    public Receive<Response> createReceive() {
        return newReceiveBuilder()
                .onMessage(OrderSuccess.class, this::onOrderSuccess)
                .onMessage(OrderFailure.class, this::onOrderFailure)
                .build();
    }

    // Behavior when order fails
    private Behavior<Response> onOrderFailure(OrderFailure failure) {
        // You can add logic here if needed, for example logging or handling failure.
        return Behaviors.stopped();
    }

    // Behavior when order is successful
    private Behavior<Response> onOrderSuccess(OrderSuccess msg) {
        // You can add logic here for handling the success, like notifying or logging.
        return Behaviors.stopped();
    }
}
