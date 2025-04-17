package com.example.Responses;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.example.ImportantActors.OneOrder;
// Order Get Response Actor

public class OrderGetResponse extends AbstractBehavior<OrderGetResponse.Response> {

    public interface Response {}

    public static class OrderSuccess implements Response {  // 🔹 Added `static`
        public OneOrder.Order order;

        public OrderSuccess(OneOrder.Order order) {
            this.order = order;
        }
    }

    public static class OrderFailure implements Response {

    }  // 🔹 Added `static`

    public OrderGetResponse(ActorContext<OrderGetResponse.Response> context) {
        super(context);
    }

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

