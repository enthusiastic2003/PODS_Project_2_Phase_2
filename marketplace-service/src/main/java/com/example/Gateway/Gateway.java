package com.example.Gateway;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import akka.actor.typed.receptionist.ServiceKey;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.StatusCodes;

import com.example.ImportantActors.*;
import com.example.Requests.OrderPostRequests;
import com.example.Responses.*;
import com.example.SerializableTraitClass;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionStage;


import static akka.http.javadsl.server.Directives.concat;

public class Gateway extends AbstractBehavior<Gateway.Command> {

    // ----- Incoming Message Protocol -----
    public interface Command {}

    public Map<Integer, Oneproduct.Product> productMap = new HashMap<>();
    public Map<Integer, OneOrder.Order>  orderMap = new HashMap<>();
    public Set<Integer> usersWhoAvailedDiscounts = new HashSet<>();


    // Initialization message for starting HTTP server
    public static final class Initialize implements Command { }

    public static final class OrderSuccess extends SerializableTraitClass implements Command {

        public int orderId;

        @JsonCreator
        public OrderSuccess(@JsonProperty("orderId") int orderId) {
            this.orderId = orderId;
        }
    }

    // Done: WORKING
    // GET /products/{productId} – forward the request to the corresponding product actor.
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
    public static final class GetProduct implements Command {
        public final int productId;
        public final ActorRef<ProductResponse> replyTo;
        @JsonCreator
        public GetProduct(int productId, ActorRef<ProductResponse> replyTo) {
            this.productId = productId;
            this.replyTo = replyTo;
        }
    }

    //Done: Working
    // GET /orders/{orderId} – forward the request to the corresponding order actor.
    public static final class GetOrder implements Command {
        public final int orderId;
        public final ActorRef<OrderGetResponse.Response> replyTo;
        public GetOrder(int orderId, ActorRef<OrderGetResponse.Response> replyTo) {
            this.orderId = orderId;
            this.replyTo = replyTo;
        }
    }

    //Done: Working
    // PUT /orders/{orderId} – forward the request (with new order status) to the corresponding order actor.
    public static final class PutOrderStatus implements Command {
        public final int order_id;
        public final OrderStatus status;
        public final ActorRef<OrderPutResponse> replyTo;

        public PutOrderStatus(int orderId, OrderStatus newStatus, ActorRef<OrderPutResponse> replyTo) {
            this.order_id = orderId;
            this.status = newStatus;
            this.replyTo = replyTo;
        }
    }

    //Done: Working
    //POST /orders
    public static final class PlaceOrder  implements Command {

        public final OrderPostRequests reqOrder;
        public final ActorRef<OrderPostResponse.Response> replyTo;

        public PlaceOrder(OrderPostRequests reqOrder, ActorRef<OrderPostResponse.Response> replyTo) {
            this.reqOrder = reqOrder;
            this.replyTo = replyTo;
        }
    }
    
    //DELETE /orders/{order_id}
    public static class DeleteOrder extends SerializableTraitClass implements Command {
        public final Integer order_id;
        public final ActorRef<OrderDelete.Response> replyTo;

        @JsonCreator
        public DeleteOrder(@JsonProperty("order_id") int order_id,@JsonProperty("replyTo") ActorRef<OrderDelete.Response> replyTo) {

            this.order_id = order_id;
            this.replyTo = replyTo;
        }

    }

    // ----- Internal State -----
    private ServerBinding serverBinding;
    public ClusterSharding sharding;
    public ActorRef<DiscountManager.Command> discountManagerRef;
    // Constructor: loads products from CSV and registers each as a cluster-sharded entity.
    // (Assumes that each product actor is created via Oneproduct.create and uses its ENTITY_KEY.)

    List<OrderItem> dummyList =  new ArrayList<>();
    public OneOrder.Order dummyOrder = new OneOrder.Order(-1, -1,  -1, OrderStatus.CANCELLED,
          dummyList  );

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(GetProduct.class, this::onGetProduct)
                .onMessage(GetOrder.class, this::onGetOrder)
                .onMessage(PutOrderStatus.class, this::onPutOrderStatus)
                .onMessage(PlaceOrder.class, this::onPlaceOrder)
                .onMessage(DeleteOrder.class, this::onDeleteOrder)
                .onMessage(OrderSuccess.class, msg -> {
                    this.orderMap.put(msg.orderId, dummyOrder);
                    return this;
                })
                .build();
    }

 // ----- Message Handlers ----


// Handler for product lookup requests
private Behavior<Command> onGetProduct(GetProduct msg) {
    int productId = msg.productId;

    // Check if product exists in local registry
    if (productId>=101 && productId<=120) {
        // Get sharded entity reference for the product
        EntityRef<Oneproduct.Command> productEntity = sharding.entityRefFor(
            Oneproduct.ENTITY_KEY, 
            String.valueOf(productId)
        );
        
        // Request details from product entity
        productEntity.tell(new Oneproduct.GetProductDetails(msg.replyTo));
    } else {
        // Immediate response if product not found
        msg.replyTo.tell(new ProductNotFound());
    }

    return this;
}

// Counter for order IDs (potential concurrency issue in clustered environment)
private Integer latestOrderId = 0;

// Handler for new order requests
private Behavior<Command> onPlaceOrder(PlaceOrder msg) {
    latestOrderId++;  // Increment order ID counter

    // Route to the order entity

    this.placeOrderRouter.tell(
    new OnePlaceOrder.placeOrderPackage(
        msg.reqOrder,
        latestOrderId,
            getContext().getSelf(),
            msg.replyTo,
            discountManagerRef
    )
    );
    getContext().getLog().info("Spawned OnePlaceOrder Actor for Order ID: {}", latestOrderId);
    
    return this;
}

// Handler for order retrieval requests
// Handler for order retrieval requests
private Behavior<Command> onGetOrder(GetOrder msg) {
    if (orderMap.containsKey(msg.orderId)) {
        // Get sharded entity reference for the order
        EntityRef<OneOrder.Command> orderEntity = sharding.entityRefFor(
                OneOrder.ENTITY_KEY,
                String.valueOf(msg.orderId)
        );

        // Use tell pattern instead of ask pattern
        orderEntity.tell(new OneOrder.GetOrderDetails(msg.replyTo));
    } else {
        msg.replyTo.tell(new OrderGetResponse.OrderFailure());
    }

    return this;
}

// Handler for order status updates
private Behavior<Command> onPutOrderStatus(PutOrderStatus msg) {
    if (orderMap.containsKey(msg.order_id)) {
        // Get sharded entity reference
        EntityRef<OneOrder.Command> orderEntity = sharding.entityRefFor(
            OneOrder.ENTITY_KEY, 
            String.valueOf(msg.order_id)
        );
        
        // Forward status update to order entity
        orderEntity.tell(new OneOrder.PutOrderStatus(msg.status, msg.replyTo));
    } else {
        // Immediate error response
        msg.replyTo.tell(new OrderPutResponse(StatusCodes.BAD_REQUEST.intValue(), "Order Not Found"));
    }

    return this;
}

// Counter for delete operation IDs
int uniqdeleteid = 0;

// Handler for order deletion requests
private Behavior<Command> onDeleteOrder(DeleteOrder msg) {
    System.out.println("onDeleteOrder "+msg.order_id);  // Consider using proper logging
    
    if (!orderMap.containsKey(msg.order_id)) {
        msg.replyTo.tell(new OrderDelete.Failure("Order ID not found in the order map"));
        return this;
    }

    this.deleteOrderRouter.tell(
            new OneDeleteOrder.setOrderDetails(
                    msg
            )
    );
    uniqdeleteid++;
    
    // Note: The actual delete command to the order entity is commented out
    // deleteOrderActor.tell(new OneOrder.DeleteOrder(msg.order_id, msg.replyTo));
    
    return this;
}


    // ----- Helper Methods -----
    // In your main actor or service class
    public static Behavior<Command> create(ServiceKey<OnePlaceOrder.Command> serviceKey, ServiceKey<OneDeleteOrder.Command> serviceKey2) {
        return Behaviors.setup(context -> {
            return new Gateway(context, serviceKey, serviceKey2);
        });
    }

    ServiceKey<OnePlaceOrder.Command> placeOrderKey;
    ActorRef<OnePlaceOrder.Command> placeOrderRouter;
    ActorRef<OneDeleteOrder.Command>  deleteOrderRouter;
    ServiceKey<OneDeleteOrder.Command> deleteOrderKey;
    // Create a router for this service key

    private Gateway(ActorContext<Command> context, ServiceKey<OnePlaceOrder.Command> serviceKey,
                    ServiceKey<OneDeleteOrder.Command> serviceKey2) {
        super(context);

        this.sharding = ClusterSharding.get(context.getSystem());

        this.discountManagerRef = getContext().spawn(DiscountManager.create(), "discountManager");
        this.placeOrderKey = serviceKey;
        this.deleteOrderKey =serviceKey2;

        this.placeOrderRouter =
                context.spawn(Routers.group(placeOrderKey).withRoundRobinRouting(), "PlaceOrderGroupRouter");

        this.deleteOrderRouter =
                context.spawn(Routers.group(deleteOrderKey).withRoundRobinRouting(), "DeleteOrderGroupRouter");

        context.getLog().info("Gateway initialized with {} products and sharding configured", productMap.size());

    }

}
