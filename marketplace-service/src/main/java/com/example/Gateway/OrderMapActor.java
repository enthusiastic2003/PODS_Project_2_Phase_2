package com.example.Gateway;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.example.ImportantActors.OneOrder;
import com.example.SerializableTraitClass;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

public class OrderMapActor extends AbstractBehavior<OrderMapActor.Command> {

    // Shared order map
    private final Map<Integer, OneOrder.Order> orderMap = new HashMap<>();

    public interface Command {}

    // Command to update the order map
    public static class UpdateOrderMap extends SerializableTraitClass implements Command {
        public final int orderId;
        public final OneOrder.Order order;

        @JsonCreator
        public UpdateOrderMap(
                @JsonProperty("orderId") int orderId,
                @JsonProperty("order") OneOrder.Order order
        ) {
            this.orderId = orderId;
            this.order = order;
        }
    }

    // Command to get the order map
    public static class GetOrderMap extends SerializableTraitClass implements Command {
        public final ActorRef<Response> replyTo;

        @JsonCreator
        public GetOrderMap(
                @JsonProperty("replyTo") ActorRef<Response> replyTo
        ) {
            this.replyTo = replyTo;
        }
    }

    public interface Response {}

    // This message will extend the SerializableTraitClass (directly)
    public static class OrderMapResponse extends SerializableTraitClass implements Response {
        public final Map<Integer, OneOrder.Order> orderMap;

        @JsonCreator
        public OrderMapResponse(
                @JsonProperty("orderMap") Map<Integer, OneOrder.Order> orderMap
        ) {
            this.orderMap = orderMap;
        }
    }

    public OrderMapActor(ActorContext<Command> context) {
        super(context);
    }

    public static Behavior<Command> create() {
        return Behaviors.setup(OrderMapActor::new);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(UpdateOrderMap.class, this::onUpdateOrderMap)
                .onMessage(GetOrderMap.class, this::onGetOrderMap)
                .build();
    }

    // Handle the update command
    private Behavior<Command> onUpdateOrderMap(UpdateOrderMap msg) {
        orderMap.put(msg.orderId, msg.order);  // Update the order map
        return this;
    }

    // Handle the get command
    private Behavior<Command> onGetOrderMap(GetOrderMap msg) {
        msg.replyTo.tell(new OrderMapResponse(new HashMap<>(orderMap)));  // Send a copy of the map back
        return this;
    }
}
