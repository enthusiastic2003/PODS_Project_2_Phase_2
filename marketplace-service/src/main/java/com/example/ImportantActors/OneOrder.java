package com.example.ImportantActors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.http.javadsl.model.StatusCodes;
import com.example.Responses.OrderDelete;
import com.example.Responses.OrderGetResponse;
import com.example.Responses.OrderPutResponse;
import com.example.SerializableTraitClass;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Actor representing a single order in the system.
 */
public class OneOrder extends AbstractBehavior<OneOrder.Command> {

    // Interface for response messages
    public interface Response{}

    /**
     * Data class representing an order's state
     */
    public static class Order extends SerializableTraitClass {
        public final Integer order_id;
        public final Integer user_id;
        public final Integer total_price;
        public OrderStatus status;
        public final List<OrderItem> items;

        @JsonCreator
        public Order(
                @JsonProperty("order_id") Integer order_id,
                @JsonProperty("user_id") Integer user_id,
                @JsonProperty("total_price") Integer total_price,
                @JsonProperty("status") OrderStatus order_status,
                @JsonProperty("items") List<OrderItem> order_items
        ) {
            this.order_id = order_id;
            this.user_id = user_id;
            this.total_price = total_price;
            this.status = order_status;
            this.items = order_items;
        }

        @Override
        public String toString() {
            return String.format("Order(id=%d, userId=%d, totalPrice=%d, status=%s, items=%d)",
                    order_id, user_id, total_price, status, items.size());
        }
    }

    // Current order state
    private Order order;

    // ----- Protocol for OneOrder Actor -----
    public interface Command {}

    // EntityTypeKey for cluster sharding identification
    public static final EntityTypeKey<Command> ENTITY_KEY =
            EntityTypeKey.create(Command.class, "OneOrder");

    /**
     * Command to retrieve order details
     */
    /**
     * Command to retrieve order details
     */
    public static class GetOrderDetails extends SerializableTraitClass implements Command {
        public final ActorRef<OrderGetResponse.Response> replyTo;

        @JsonCreator
        public GetOrderDetails(@JsonProperty("replyTo") ActorRef<OrderGetResponse.Response> replyTo) {
            this.replyTo = replyTo;
        }
    }

    /**
     * Command to update order status
     */
    public static class PutOrderStatus extends SerializableTraitClass implements Command {
        public final OrderStatus order_status;
        public final ActorRef<OrderPutResponse> replyTo;

        @JsonCreator
        public PutOrderStatus(
                @JsonProperty("order_status") OrderStatus orderStatus,
                @JsonProperty("replyTo") ActorRef<OrderPutResponse> replyTo
        ) {
            this.order_status = orderStatus;
            this.replyTo = replyTo;
        }
    }

    /**
     * Command to initialize/update order state
     */
    public static class SetOrder extends SerializableTraitClass implements Command {
        public final Order order;

        @JsonCreator
        public SetOrder(@JsonProperty("order") Order order) {
            this.order = order;
        }
    }

    /**
     * Command to delete/cancel an order
     */
    public static class DeleteOrder extends SerializableTraitClass implements Command {
        public final ActorRef<OrderDelete.Response> replyTo;

        @JsonCreator
        public DeleteOrder(@JsonProperty("replyTo") ActorRef<OrderDelete.Response> replyTo) {
            this.replyTo = replyTo;
        }
    }

    // Private constructor
    private OneOrder(ActorContext<Command> context) {
        super(context);
        getContext().getLog().info("OneOrder actor created");
    }

    // Factory method to create the actor
    public static Behavior<Command> create() {
        return Behaviors.setup(OneOrder::new);
    }

    // Define message handlers
    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(GetOrderDetails.class, this::onGetOrderDetails)
                .onMessage(PutOrderStatus.class, this::onPutOrderStatus)
                .onMessage(SetOrder.class, this::onSetOrder)
                .onMessage(DeleteOrder.class, this::onDeleteOrder)
                .build();
    }

    /**
     * Handles order deletion/cancellation requests
     */
    private Behavior<Command> onDeleteOrder(DeleteOrder command) {
        if (this.order == null) {
            getContext().getLog().error("Cannot delete order: order state is null");
            command.replyTo.tell(new OrderDelete.Failure("Order not initialized"));
            return this;
        }

        getContext().getLog().info("Processing delete request for order {}", order.order_id);

        if (this.order.status != OrderStatus.PLACED) {
            getContext().getLog().warn("Order deletion failed: order status is {}, not PLACED",
                    this.order.status);
            command.replyTo.tell(new OrderDelete.Failure(
                    "Order Deletion Failed because order status not placed"));
        } else {
            this.order.status = OrderStatus.CANCELLED;
            getContext().getLog().info("Order {} successfully changed to CANCELLED", order.order_id);
            command.replyTo.tell(new OrderDelete.Success("Order Deleted"));
        }
        return this;
    }

    /**
     * Initializes or updates order state
     */
    private Behavior<Command> onSetOrder(SetOrder msg) {
        this.order = msg.order;
        getContext().getLog().info("Order state set: {}", this.order);
        return this;
    }

    /**
     * Handles order details requests
     */
    /**
     * Handles order details requests
     */
    private Behavior<Command> onGetOrderDetails(GetOrderDetails msg) {
        if (this.order == null) {
            getContext().getLog().error("Cannot get order details: order state is null");
            msg.replyTo.tell(new OrderGetResponse.OrderFailure());
            return this;
        }

        getContext().getLog().info("Sending order details for order {}", order.order_id);
        msg.replyTo.tell(new OrderGetResponse.OrderSuccess(this.order));
        return this;
    }

    /**
     * Handles order status update requests
     */
    private Behavior<Command> onPutOrderStatus(PutOrderStatus msg) {
        if (this.order == null) {
            getContext().getLog().error("Cannot update order status: order state is null");
            msg.replyTo.tell(new OrderPutResponse(
                    StatusCodes.BAD_REQUEST.intValue(), "Order not initialized"));
            return this;
        }

        if (msg.order_status != OrderStatus.DELIVERED) {
            getContext().getLog().warn("Invalid order status update request: {} -> {}",
                    this.order.status, msg.order_status);
            msg.replyTo.tell(new OrderPutResponse(
                    StatusCodes.BAD_REQUEST.intValue(), "Invalid Status set request"));
            return this;
        }

        if (this.order.status != OrderStatus.PLACED) {
            getContext().getLog().warn("Order status update failed: current status {} not PLACED",
                    this.order.status);
            msg.replyTo.tell(new OrderPutResponse(
                    StatusCodes.BAD_REQUEST.intValue(), "Order status not PLACED"));
            return this;
        }

        this.order.status = OrderStatus.DELIVERED;
        getContext().getLog().info("Order {} status updated to DELIVERED", order.order_id);
        msg.replyTo.tell(new OrderPutResponse(StatusCodes.OK.intValue(), "Order Delivered"));
        return this;
    }
}