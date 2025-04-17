package com.example.Responses;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.http.javadsl.model.StatusCode;
import com.example.ImportantActors.OrderItem;
import com.example.ImportantActors.OrderStatus;
import com.example.Requests.OrderItemRequests;

import java.util.List;

// OrderPostResponse Actor
public class OrderPostResponse extends AbstractBehavior<OrderPostResponse.Response> {

    public interface Response {}

    // Message to indicate order success with details
    public static class OrderSuccess implements Response {
        public final int order_id;
        public final int user_id;
        public final int total_price;
        public final OrderStatus status;
        public final List<OrderItem> items;

        public OrderSuccess(int orderId, int userId, int totalPrice, OrderStatus stats, List<OrderItem> items) {
            this.order_id = orderId;
            this.user_id = userId;
            this.total_price = totalPrice;
            this.items = items;
            this.status = stats;
        }
    }

    public static class OrderFailure implements Response {
        public final StatusCode statusCode;
        public final String message;
        public OrderFailure(StatusCode statusCode, String message) {
            this.statusCode = statusCode;
            this.message = message;
        }
    }

    public OrderPostResponse(ActorContext<Response> context) {
        super(context);
    }

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

    private Behavior<Response> onOrderFailure(OrderFailure failure) {
        return Behaviors.stopped();
    }

    private Behavior<Response> onOrderSuccess(OrderSuccess msg) {
        return Behaviors.stopped();
    }


}
