package com.example.ImportantActors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.model.HttpMethods;
import akka.http.javadsl.model.HttpRequest;
import com.example.Gateway.Gateway;
import com.example.Responses.OrderDelete;
import com.example.Responses.OrderGetResponse;
import com.example.SerializableTraitClass;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.EntityRef;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// Actor responsible for handling order deletion process
public class OneDeleteOrder extends AbstractBehavior<OneDeleteOrder.Command> {

    // Commands interface
    public interface Command {}

    // Command to initiate order deletion process
    public static final class StartDeleteProcess implements Command {
        // Empty command to start the process
    }

    // Command received when order details are received
    public static final class OrderDetailsReceived implements Command {
        public final OneOrder.Order order;

        public OrderDetailsReceived(OneOrder.Order order) {
            this.order = order;
        }
    }

    // Command received when order is deleted
    public static final class OrderDeleted implements Command {
        public final OrderDelete.Response response;

        public OrderDeleted(OrderDelete.Response response) {
            this.response = response;
        }
    }

    // Command received when product is restocked
    public static final class ProductRestocked implements Command {
        public final int productId;
        public final int quantity;

        public ProductRestocked(int productId, int quantity) {
            this.productId = productId;
            this.quantity = quantity;
        }
    }

    // Command received when wallet is updated
    public static final class WalletUpdated implements Command {
        public final boolean success;

        public WalletUpdated(boolean success) {
            this.success = success;
        }
    }

    public static final class setOrderDetails extends SerializableTraitClass implements Command {
        Gateway.DeleteOrder msg;

        @JsonCreator
        public setOrderDetails(@JsonProperty("msg") Gateway.DeleteOrder msg) {
            this.msg = msg;
        }
    }


    // Logger for the actor
    private static final Logger logger = LoggerFactory.getLogger(OneDeleteOrder.class);

    // Factory method to create the actor
    public static Behavior<Command> create() {
        return Behaviors.setup(context -> new OneDeleteOrder(context));
    }

    // Actor state and dependencies
    private  Gateway.DeleteOrder msg;  // Original delete order message
    private final ClusterSharding sharding;  // Cluster sharding reference
    private Http http;  // HTTP client for external calls
    private OneOrder.Order orderDetails;  // Store order details when received
    private int productsRestocked = 0;  // Counter to track restocked products
    private boolean walletUpdated = false;  // Flag to track wallet update

    // Constructor
    private OneDeleteOrder(ActorContext<Command> context) {
        super(context);
        //this.msg = orderMsg;
        this.sharding = ClusterSharding.get(context.getSystem());
        this.http = Http.get(context.getSystem());  // Initialize HTTP client
        // Self-send a message to start the process
        // context.getSelf().tell(new StartDeleteProcess());
    }

    // Message handler
    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(StartDeleteProcess.class, this::onStartDeleteProcess)
                .onMessage(OrderDetailsReceived.class, this::onOrderDetailsReceived)
                .onMessage(OrderDeleted.class, this::onOrderDeleted)
                .onMessage(ProductRestocked.class, this::onProductRestocked)
                .onMessage(WalletUpdated.class, this::onWalletUpdated)
                .onMessage(setOrderDetails.class, this::onSetOrderDetails)
                .build();
    }

    private Behavior<Command> onSetOrderDetails(setOrderDetails cmd) {

            this.msg = cmd.msg;
            getContext().getSelf().tell(new StartDeleteProcess());
            return this;
    }


    // Handler for starting the deletion process
// Handler for starting the deletion process
    private Behavior<Command> onStartDeleteProcess(StartDeleteProcess cmd) {
        logger.info("Starting delete process for order: {}", msg.order_id);

        // Step 1: Request order details
        EntityRef<OneOrder.Command> orderEntity = sharding.entityRefFor(
                OneOrder.ENTITY_KEY,
                String.valueOf(msg.order_id)
        );

        // Create a new message adapter that converts from OrderGetResponse.Response to OrderDetailsReceived
        ActorRef<OrderGetResponse.Response> responseAdapter = getContext().messageAdapter(
                OrderGetResponse.Response.class,
                response -> {
                    if (response instanceof OrderGetResponse.OrderSuccess) {
                        return new OrderDetailsReceived(((OrderGetResponse.OrderSuccess) response).order);
                    } else {
                        // Handle failure case - create a dummy order
                        logger.error("Failed to get order details for order ID: {}", msg.order_id);
                        List<OrderItem> emptyItems = new ArrayList<>();
                        return new OrderDetailsReceived(
                                new OneOrder.Order(-1, -1, 0, OrderStatus.UNKNOWN, emptyItems)
                        );
                    }
                }
        );

        // Request the order details using the adapter
        orderEntity.tell(new OneOrder.GetOrderDetails(responseAdapter));

        return this;
    }

    // Handler for when order details are received
    private Behavior<Command> onOrderDetailsReceived(OrderDetailsReceived cmd) {
        logger.info("Received order details for order ID: {}", msg.order_id);

        // Store order details for later use
        this.orderDetails = cmd.order;

        if(this.orderDetails.order_id<0){
            this.msg.replyTo.tell(new OrderDelete.Failure("Order not found " +  this.msg.order_id));
            return  Behaviors.empty();
        }

        // Step 2: Send delete command to order entity
        EntityRef<OneOrder.Command> orderEntity = sharding.entityRefFor(
                OneOrder.ENTITY_KEY,
                String.valueOf(msg.order_id)
        );

        // Create an adapter that will convert the OrderDelete.Response to our OrderDeleted command
        ActorRef<OrderDelete.Response> deleteAdapter = getContext().messageAdapter(
                OrderDelete.Response.class,
                OrderDeleted::new
        );

        // Request the order deletion
        orderEntity.tell(new OneOrder.DeleteOrder(deleteAdapter));

        return this;
    }

    // Handler for when order is deleted
    private Behavior<Command> onOrderDeleted(OrderDeleted cmd) {
        // Check if deletion was successful
        if (cmd.response instanceof OrderDelete.Failure) {
            logger.error("Order deletion failed: {}", ((OrderDelete.Failure) cmd.response).message);
            // Notify the original sender of failure
            msg.replyTo.tell(new OrderDelete.Failure(((OrderDelete.Failure) cmd.response).message));
            return Behaviors.stopped();
        }

        logger.info("Order {} successfully marked as CANCELLED", msg.order_id);

        // Step 3: Restock products
        List<OrderItem> orderItems = orderDetails.items;
        for (OrderItem item : orderItems) {
            EntityRef<Oneproduct.Command> productEntity = sharding.entityRefFor(
                    Oneproduct.ENTITY_KEY,
                    String.valueOf(item.product_id)
            );

            // Define a message adapter to handle restocking response
            ActorRef<Oneproduct.RestockConfirmation> restockAdapter = getContext().messageAdapter(
                    Oneproduct.RestockConfirmation.class,
                    resp -> new ProductRestocked(resp.productId, resp.quantity)
            );

            // Add quantity back to product stock
            productEntity.tell(new Oneproduct.AddToStockForDeleteOrder(
                    item.quantity,
                    restockAdapter
            ));

            logger.info("Requested restock for product ID {} with quantity {}",
                    item.product_id, item.quantity);
        }

        // Step 4: Refund user's wallet
        Integer userId = orderDetails.user_id;
        String walletServiceUrl = getContext().getSystem().settings().config()
                .getString("my-app.routes.wallet-service");
        Integer amount = orderDetails.total_price;

        try {
            // Create JSON payload for wallet credit
            String jsonPayload = String.format(
                    "{\"action\": \"credit\", \"amount\": %d}",
                    amount
            );

            // Build HTTP request to wallet service
            HttpRequest walletRequest = HttpRequest.create()
                    .withMethod(HttpMethods.PUT)
                    .withUri(walletServiceUrl + userId)
                    .withEntity(HttpEntities.create(
                            ContentTypes.APPLICATION_JSON,
                            jsonPayload
                    ));

            // Set up an adapter to handle HTTP response
            ActorRef<Boolean> walletAdapter = getContext().messageAdapter(
                    Boolean.class,
                    WalletUpdated::new
            );

            // Send request and handle response
            http.singleRequest(walletRequest).thenAccept(response -> {
                boolean success = response.status().isSuccess();
                response.discardEntityBytes(getContext().getSystem());
                walletAdapter.tell(success);
            });

        } catch (Exception e) {
            logger.error("Failed to call wallet service", e);
            walletUpdated = true; // Mark as done even though failed
            checkCompletion();
        }

        return this;
    }

    // Handler for product restock confirmation
    private Behavior<Command> onProductRestocked(ProductRestocked cmd) {
        logger.info("Confirmed restock for product ID {} with quantity {}",
                cmd.productId, cmd.quantity);

        productsRestocked++;
        checkCompletion();
        return this;
    }

    // Handler for wallet update confirmation
    private Behavior<Command> onWalletUpdated(WalletUpdated cmd) {
        logger.info("Wallet update completed with success: {}", cmd.success);

        walletUpdated = true;
        checkCompletion();
        return this;
    }

    // Helper method to check if all operations are complete
    private Behavior<Command> checkCompletion() {
        // Check if all products have been restocked and wallet has been updated
        if (walletUpdated && productsRestocked == orderDetails.items.size()) {
            logger.info("Order deletion process completed successfully for order ID: {}", msg.order_id);
            // Notify the original sender of success
            msg.replyTo.tell(new OrderDelete.Success("Order deleted"));
            // Stop the actor as its work is done

        }
        return  Behaviors.stopped();
    }
}