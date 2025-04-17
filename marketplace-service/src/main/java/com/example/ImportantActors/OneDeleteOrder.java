package com.example.ImportantActors;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.model.HttpMethods;
import akka.http.javadsl.model.HttpRequest;
import com.example.Gateway.Gateway;
import com.example.Responses.OrderDelete;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.EntityRef;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

// Actor responsible for handling order deletion process
public class OneDeleteOrder extends AbstractBehavior<OneDeleteOrder.Command> {

    // Interface for commands this actor can handle
    public interface Command {}

    // Response message class for deletion operations
    public static final class Response {
        public final String message;
        public Response(String message) {
            this.message = message;
        }
    }

    // Logger for the actor
    private static final Logger logger = LoggerFactory.getLogger(OneDeleteOrder.class);

    // Factory method to create the actor
    public static Behavior<Command> create(Gateway.DeleteOrder orderId, ClusterSharding sharding, 
                                         Map<Integer, Oneproduct.Product> productMap) {
        return Behaviors.setup(context -> new OneDeleteOrder(context, orderId, sharding, productMap));
    }

    // Actor state and dependencies
    private final Gateway.DeleteOrder msg;  // Original delete order message
    private final ClusterSharding sharding;  // Cluster sharding reference
    Http http;  // HTTP client for external calls
    private final Map<Integer, Oneproduct.Product> productMap;  // Product reference data

    // Constructor
    private OneDeleteOrder(ActorContext<Command> context, Gateway.DeleteOrder OrderId, 
                         ClusterSharding sharding, Map<Integer, Oneproduct.Product> productMap) {
        super(context);
        this.msg = OrderId;
        this.sharding = sharding;
        this.productMap = productMap;
        
        // Debug logging (consider using logger instead of System.out)
        System.out.println("Order ID: " + msg);
        System.out.println("Product Map: " + productMap);
        System.out.println("Sharding: " + sharding);
        logger.info("DeleteOrder actor constructor");

        this.http = Http.get(context.getSystem());  // Initialize HTTP client
        deleteOrder();  // Start deletion process immediately
    }

    // Main order deletion logic
    private void deleteOrder(){
        // Get reference to the order entity in the cluster
        EntityRef<OneOrder.Command> newOrder = sharding.entityRefFor(
            OneOrder.ENTITY_KEY, 
            String.valueOf(msg.order_id)
        );

        // Get current order details using ask pattern
        CompletionStage<OneOrder.Order> reponseOrders = AskPattern.ask(
            newOrder,
            replyTo -> new OneOrder.GetOrderDetails(replyTo),
            Duration.ofSeconds(3),  // Timeout for safety
            getContext().getSystem().scheduler()
        );

        // Blocking wait for order details (potential improvement: make async)
        OneOrder.Order order = reponseOrders.toCompletableFuture().join();

        // Send delete command to order entity
        CompletionStage<OrderDelete.Response> responseFuture = AskPattern.ask(
            newOrder,
            replyTo -> new OneOrder.DeleteOrder(replyTo),
            Duration.ofSeconds(3),
            getContext().getSystem().scheduler()
        );

        // Process delete response
        OrderDelete.Response response = responseFuture.toCompletableFuture().join();

        // Handle failure case
        if (response instanceof OrderDelete.Failure) {
            msg.replyTo.tell(new OrderDelete.Failure(((OrderDelete.Failure) response).message));
            return;
        }

        // Restock products from cancelled order
        List<OrderItem> current_order_items = order.items;
        for (OrderItem item : current_order_items) {
            EntityRef<Oneproduct.Command> productEntity = sharding.entityRefFor(
                Oneproduct.ENTITY_KEY, 
                String.valueOf(item.product_id)
            );
            // Add quantity back to product stock
            productEntity.tell(new Oneproduct.AddToStockForDeleteOrder(
                item.quantity, 
                getContext().getSelf()
            ));
            System.out.println("Restocked product ID " + item.product_id + 
                              " with quantity " + item.quantity);
        }

        // Refund user's wallet
        Integer userId = order.user_id;
        String walletServiceUrl = getContext().getSystem().settings().config()
            .getString("my-app.routes.wallet-service");
        Integer amount = order.total_price;

        try {
            // Create JSON payload for wallet credit
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> requestBodyMap = Map.of(
                "action", "credit",
                "amount", amount
            );
            String requestBody = objectMapper.writeValueAsString(requestBodyMap);
            System.out.println("Request body: " + requestBody);

            // Alternative JSON construction
            String jsonPayload = String.format(
                "{\"action\": \"credit\", \"amount\": %d}", 
                (int)amount
            );

            // Build HTTP request to wallet service
            HttpRequest walletRequest = HttpRequest.create()
                .withMethod(HttpMethods.PUT)
                .withUri(walletServiceUrl + userId)
                .withEntity(HttpEntities.create(
                    ContentTypes.APPLICATION_JSON, 
                    jsonPayload
                ));

            // Send request and handle response
            http.singleRequest(walletRequest).thenCompose(response2 -> {
                if (response2.status().isSuccess()) {
                    System.out.println("Wallet service response: " + response.toString());
                    response2.discardEntityBytes(getContext().getSystem());
                    return CompletableFuture.completedFuture(true);
                } else {
                    response2.discardEntityBytes(getContext().getSystem());
                    return CompletableFuture.completedFuture(false);
                }
            });

        } catch (Exception e) {
            logger.error("Failed to call wallet service", e);
        }

        // Notify original sender of success
        msg.replyTo.tell(new OrderDelete.Success("Order deleted"));
    }

    // Message handler (empty in this case as all work is done in constructor)
    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder().build();
    }
}