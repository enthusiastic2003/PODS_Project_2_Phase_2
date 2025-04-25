package com.example.Responses;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.example.ImportantActors.OneOrder;
import com.example.SerializableTraitClass;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

public class OrderGetResponse extends AbstractBehavior<OrderGetResponse.Response> {

    public interface Response {}

    public static class OrderSuccess extends SerializableTraitClass implements Response {
        @JsonProperty("order")  // Add this annotation for JSON serialization
        public OneOrder.Order order;

        // Constructor for JSON deserialization
        public OrderSuccess(@JsonProperty("order") OneOrder.Order order) {
            this.order = order;
        }
    }

    public static class OrderFailure extends SerializableTraitClass implements Response {
        // No properties, can be simply an empty class
    }

    // Constructor
    public OrderGetResponse(ActorContext<OrderGetResponse.Response> context) {
        super(context);
    }

    // Factory method to create a new instance of this actor
    public static Behavior<OrderGetResponse.Response> create() {
        return Behaviors.setup(OrderGetResponse::new);
    }

    @Override
    public Receive<OrderGetResponse.Response> createReceive() {
        return newReceiveBuilder()
                .onMessage(OrderGetResponse.OrderSuccess.class, this::onOrderSuccess)
                .onMessage(OrderGetResponse.OrderFailure.class, this::onOrderFailure)
                .build();
    }

    private Behavior<OrderGetResponse.Response> onOrderFailure(OrderGetResponse.OrderFailure failure) {
        return Behaviors.stopped();
    }

    private Behavior<OrderGetResponse.Response> onOrderSuccess(OrderGetResponse.OrderSuccess msg) {
        return Behaviors.stopped();
    }
}
