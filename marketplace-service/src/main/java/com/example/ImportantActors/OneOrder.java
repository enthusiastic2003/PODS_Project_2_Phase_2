package com.example.ImportantActors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.http.javadsl.model.StatusCodes;
import com.example.Responses.OrderDelete;
import com.example.Responses.OrderPutResponse;

import java.util.List;

/**
 * Actor representing a single order in the system.
 * Handles order state management including:
 * - Retrieving order details
 * - Updating order status
 * - Order cancellation
 * - Order initialization
 */
public class OneOrder extends AbstractBehavior<OneOrder.Command> {

    // Interface for response messages
    public interface Response {}

    /**
     * Data class representing an order's state
     */
    public static class Order {
        public Integer order_id;
        public Integer user_id;
        public Integer total_price;
        public OrderStatus status;
        public List<OrderItem> items;

        public Order(Integer order_id, Integer user_id, Integer total_price,
                     OrderStatus order_status, List<OrderItem> order_items) {
            this.order_id = order_id;
            this.user_id = user_id;
            this.total_price = total_price;
            this.status = order_status;
            this.items = order_items;
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
    public static class GetOrderDetails implements Command {
        public final ActorRef<OneOrder.Order> replyTo;
        public GetOrderDetails(ActorRef<OneOrder.Order> replyTo) {
            this.replyTo = replyTo;
        }
    }

    /**
     * Command to update order status
     */
    public static class PutOrderStatus implements Command {
        public final OrderStatus order_status;
        public final ActorRef<OrderPutResponse> replyTo;
        public PutOrderStatus(OrderStatus orderStatus, ActorRef<OrderPutResponse> replyTo) {
            this.order_status = orderStatus;
            this.replyTo = replyTo;
        }
    }

    /**
     * Command to initialize/update order state
     */
    public static class SetOrder implements Command {
        public final Order order;
        public SetOrder(Order order) {
            this.order = order;
        }
    }

    /**
     * Command to send order details (similar to GetOrderDetails)
     */
    public static class SendOrders implements Command {
        public final ActorRef<OneOrder.Order> replyTo;
        public SendOrders(ActorRef<OneOrder.Order> replyTo) {
            this.replyTo = replyTo;
        }
    }

    /**
     * Command to delete/cancel an order
     */
    public static class DeleteOrder implements Command {
        public final ActorRef<OrderDelete.Response> replyTo;
        public DeleteOrder(ActorRef<OrderDelete.Response> replyTo) {
            this.replyTo = replyTo;
        }
    }

    // Private constructor
    private OneOrder(ActorContext<Command> context) {
        super(context);
    }

    // Factory method to create the actor
    public static Behavior<Command> create() {
        return Behaviors.setup(context -> new OneOrder(context));
    }

    // Define message handlers
    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(GetOrderDetails.class, this::onGetOrderDetails)
                .onMessage(PutOrderStatus.class, this::onPutOrderDetails)
                .onMessage(SetOrder.class, this::onSetOrder)
                .onMessage(SendOrders.class, this::onSendOrders)
                .onMessage(DeleteOrder.class, this::onDeleteOrder)
                .build();
    }

    /**
     * Handles order deletion/cancellation requests
     */
    public Behavior<Command> onDeleteOrder(DeleteOrder command) {
        // Only allow cancellation if order is in PLACED status
        if (this.order.status != OrderStatus.PLACED) {
            command.replyTo.tell(new OrderDelete.Failure(
                    "Order Deletion Failed because order status not placed"));
        } else {
            // Mark order as cancelled
            this.order.status = OrderStatus.CANCELLED;
            command.replyTo.tell(new OrderDelete.Success("Order Deleted"));
        }
        return this;
    }

    /**
     * Handles order details requests (similar to GetOrderDetails)
     */
    public Behavior<Command> onSendOrders(SendOrders msg) {
        msg.replyTo.tell(this.order);
        return this;
    }

    /**
     * Initializes or updates order state
     */
    private Behavior<Command> onSetOrder(SetOrder msg) {
        this.order = msg.order;
        return this;
    }

    /**
     * Handles order details requests
     */
    private Behavior<Command> onGetOrderDetails(GetOrderDetails msg) {
        msg.replyTo.tell(order);
        return this;
    }

    /**
     * Handles order status update requests
     */
    private Behavior<Command> onPutOrderDetails(PutOrderStatus msg) {
        // Currently only allows transition to DELIVERED status
        if (msg.order_status != OrderStatus.DELIVERED) {
            msg.replyTo.tell(new OrderPutResponse(
                    StatusCodes.BAD_REQUEST, "Invalid Status set request"));
            return this;
        }

        // Only allow status update if order is PLACED
        if (this.order.status != OrderStatus.PLACED) {
            msg.replyTo.tell(new OrderPutResponse(
                    StatusCodes.BAD_REQUEST, "Order status not PLACED"));
            return this;
        }

        // Update status to DELIVERED
        this.order.status = OrderStatus.DELIVERED;
        msg.replyTo.tell(new OrderPutResponse(StatusCodes.OK, "Order Delivered"));
        return this;
    }
}