package com.example.ImportantActors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.*;
import com.example.Gateway.Gateway;
import com.example.Requests.OrderItemRequests;
import com.example.Requests.OrderPostRequests;
import com.example.Responses.*;
import com.example.SerializableTraitClass;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import akka.http.javadsl.unmarshalling.Unmarshaller;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class OnePlaceOrder extends AbstractBehavior<OnePlaceOrder.Command> {

    public interface Command {}
    public interface Response {}

    public static class Stop implements Command {} // Stop command

    // New messages for tell-based state machine
    public static class StockReservationResponse extends SerializableTraitClass implements Command {
        public final int productId;
        public final boolean success;
        public final String errorMessage;

        @JsonCreator
        public StockReservationResponse(
                @JsonProperty("productId") int productId,
                @JsonProperty("success") boolean success,
                @JsonProperty("errorMessage") String errorMessage) {
            this.productId = productId;
            this.success = success;
            this.errorMessage = errorMessage;
        }
    }

    public static class ProductDetailsResponse extends SerializableTraitClass implements Command {
        public final int productId;
        public final Oneproduct.Product product;

        @JsonCreator
        public ProductDetailsResponse(
                @JsonProperty("productId") int productId,
                @JsonProperty("product") Oneproduct.Product product) {
            this.productId = productId;
            this.product = product;
        }
    }

    // New messages for discount management
    public static class DiscountStatusResponse extends SerializableTraitClass implements Command {
        public final Integer userId;
        public final boolean discountAvailed;

        @JsonCreator
        public DiscountStatusResponse(
                @JsonProperty("userId") Integer userId,
                @JsonProperty("discountAvailed") boolean discountAvailed) {
            this.userId = userId;
            this.discountAvailed = discountAvailed;
        }
    }

    public static class ApplyDiscountResponse extends SerializableTraitClass implements Command {
        public final Integer userId;
        public final boolean discountApplied;

        @JsonCreator
        public ApplyDiscountResponse(
                @JsonProperty("userId") Integer userId,
                @JsonProperty("discountApplied") boolean discountApplied) {
            this.userId = userId;
            this.discountApplied = discountApplied;
        }
    }

    // New message for payment response
    public static class PaymentResponse extends SerializableTraitClass implements Command {
        public final Integer userId;
        public final boolean success;
        public final double amount;

        @JsonCreator
        public PaymentResponse(
                @JsonProperty("userId") Integer userId,
                @JsonProperty("success") boolean success,
                @JsonProperty("amount") double amount) {
            this.userId = userId;
            this.success = success;
            this.amount = amount;
        }
    }

    // Tracking state for order processing
    private static class OrderProcessingState {
        Map<Integer, Boolean> stockReservations = new HashMap<>();
        Map<Integer, Oneproduct.Product> productDetails = new HashMap<>();
        double totalCost = 0.0;
        int pendingReservations = 0;
        int pendingPriceQueries = 0;

        public boolean allReservationsComplete() {
            return pendingReservations == 0;
        }

        public boolean allPriceQueriesComplete() {
            return pendingPriceQueries == 0;
        }

        public boolean allReservationsSuccessful() {
            for (Boolean success : stockReservations.values()) {
                if (!success) return false;
            }
            return true;
        }
    }

    private ClusterSharding sharding;
    private OrderProcessingState processingState = new OrderProcessingState();
    private OrderPostRequests orderRequests;
    public Integer portUserService = 8080;
    public Integer portWalletService = 8082;
    private ActorRef<OrderPostResponse.Response> replyTo;
    private Boolean discountStatus = false;
    private ActorRef<Gateway.Command> prevActor;
    public ActorRef<DiscountManager.Command> discountManagerRef;
    private Integer orderId;
    private Http http;
    private double finalCost = 0.0;

    private OnePlaceOrder(ActorContext<Command> context){
        super(context);
        http = Http.get(context.getSystem());
        sharding = ClusterSharding.get(context.getSystem());
    }

    public static class placeOrderPackage extends SerializableTraitClass implements Command {
        public final OrderPostRequests orderRequests;
        public final Integer orderId;
        public final ActorRef<Gateway.Command> orderMap;
        public final ActorRef<OrderPostResponse.Response> replyTo;
        public final ActorRef<DiscountManager.Command> discountManagerRef;

        @JsonCreator
        public placeOrderPackage(
                @JsonProperty("orderRequests") OrderPostRequests orderRequests,
                @JsonProperty("orderId") Integer orderId,
                @JsonProperty("orderMap") ActorRef<Gateway.Command> orderMap,
                @JsonProperty("replyTo") ActorRef<OrderPostResponse.Response> replyTo,
                @JsonProperty("discountManagerRef") ActorRef<DiscountManager.Command> discountManagerRef
        ) {
            this.orderRequests = orderRequests;
            this.orderId = orderId;
            this.orderMap = orderMap;
            this.replyTo = replyTo;
            this.discountManagerRef = discountManagerRef;
        }
    }

    public static Behavior<Command> create() {
        return Behaviors.setup(context -> new OnePlaceOrder(context));
    }

    private Behavior<Command> processOrder() {
        getContext().getLog().info("Processing order: {}", orderRequests);
        getContext().getLog().info("ARG PORT: " + getContext().getSystem().settings().config().getInt("akka.remote.artery.canonical.port"));

        // Step 1: Validate user - keeping the CompletableFuture approach for HTTP requests
        CompletionStage<Boolean> validationResult = validateUser(orderRequests.user_id);
        validationResult.thenAccept(valid -> {
            if (valid) {
                // Get discount status using tell pattern
                ActorRef<OnePlaceOrder.Command> self = getContext().getSelf();
                discountManagerRef.tell(new DiscountManager.GetDiscountStatusTell(orderRequests.user_id, discountStatus, self));
            } else {
                stopWithFailure("User validation failed", StatusCodes.BAD_REQUEST);
            }
        });

        return this;
    }

    private void handleDiscountStatusResponse(DiscountStatusResponse response) {
        discountStatus = response.discountAvailed;
        System.out.println("User validated, discount status: " + discountStatus);

        // Start the stock reservation process
        initiateStockReservation();
    }

    private void initiateStockReservation() {
        // Reset state for new order processing
        processingState = new OrderProcessingState();
        processingState.pendingReservations = orderRequests.items.size();

        for (OrderItemRequests item : orderRequests.items) {
            int pid = item.product_id;
            int quantity = item.quantity;

            // Skip if product doesn't exist
            if (!((pid >= 101) && (pid <= 120))) {
                processingState.stockReservations.put(pid, false);
                processingState.pendingReservations--;
                checkReservationCompletion();
                continue;
            }

            // Get product entity ref
            EntityRef<Oneproduct.Command> productEntity =
                    sharding.entityRefFor(Oneproduct.ENTITY_KEY, String.valueOf(pid));

            // Tell the product actor to reserve stock - it will respond back to us
            ActorRef<OnePlaceOrder.Command> self = getContext().getSelf();
            productEntity.tell(new Oneproduct.ReserveStockTell(quantity, pid, self));
        }
    }

    private void handleStockReservationResponse(StockReservationResponse response) {
        processingState.stockReservations.put(response.productId, response.success);
        processingState.pendingReservations--;

        getContext().getLog().info("Received stock reservation response for product {}: {}",
                response.productId, response.success);

        checkReservationCompletion();
    }

    private void checkReservationCompletion() {
        if (processingState.allReservationsComplete()) {
            getContext().getLog().info("All stock reservations complete");

            if (processingState.allReservationsSuccessful()) {
                getContext().getLog().info("All reservations successful, calculating total cost");
                initiateTotalCostCalculation();
            } else {
                // If any reservation failed, release all reserved stock
                releaseReservedStock();
                stopWithFailure("Product not found or stock insufficient", StatusCodes.BAD_REQUEST);
            }
        }
    }

    private void initiateTotalCostCalculation() {
        // Reset pending price queries counter
        processingState.pendingPriceQueries = orderRequests.items.size();

        for (OrderItemRequests item : orderRequests.items) {
            int pid = item.product_id;

            // Get product entity ref
            EntityRef<Oneproduct.Command> productEntity =
                    sharding.entityRefFor(Oneproduct.ENTITY_KEY, String.valueOf(pid));

            // Tell the product actor to send product details
            ActorRef<OnePlaceOrder.Command> self = getContext().getSelf();
            productEntity.tell(new Oneproduct.GetProductDetailsTell(pid, self));
        }
    }

    private void handleProductDetailsResponse(ProductDetailsResponse response) {
        OrderItemRequests matchingItem = null;
        for (OrderItemRequests item : orderRequests.items) {
            if (item.product_id == response.productId) {
                matchingItem = item;
                break;
            }
        }

        if (matchingItem != null && response.product != null) {
            processingState.productDetails.put(response.productId, response.product);
            processingState.totalCost += response.product.price * matchingItem.quantity;
        }

        processingState.pendingPriceQueries--;

        checkPriceQueryCompletion();
    }

    private void checkPriceQueryCompletion() {
        if (processingState.allPriceQueriesComplete()) {
            getContext().getLog().info("All price queries complete, total cost: {}", processingState.totalCost);

            // Apply discount if applicable - using tell pattern
            ActorRef<OnePlaceOrder.Command> self = getContext().getSelf();
            discountManagerRef.tell(new DiscountManager.ApplyDiscountTell(orderRequests.user_id, self));
        }
    }

    private void handleApplyDiscountResponse(ApplyDiscountResponse response) {
        discountStatus = response.discountApplied;
        finalCost = processingState.totalCost;

        if (discountStatus) {
            finalCost = finalCost * 0.90;
        }

        // Process payment using HTTP client
        processPayment(orderRequests.user_id, finalCost).thenAccept(paymentSuccess -> {
            if (paymentSuccess) {
                finalizeOrder(finalCost);
            } else {
                // If payment fails, release the reserved stock
                releaseReservedStock();
                discountManagerRef.tell(new DiscountManager.RevertDiscountLock(orderRequests.user_id));
                stopWithFailure("Payment processing failed", StatusCodes.BAD_REQUEST);
            }
        });
    }

    private void finalizeOrder(double totalCost) {
        System.out.println("Payment processed successfully");

        if (!discountStatus) {
            discountManagerRef.tell(new DiscountManager.ReleaseDiscount(orderRequests.user_id));
        }

        // Create order items
        List<OrderItem> orderItems = new ArrayList<>();
        int id = 1;
        for (OrderItemRequests orderItemRequests : orderRequests.items) {
            int pid = orderItemRequests.product_id;
            int quant = orderItemRequests.quantity;
            orderItems.add(new OrderItem(id, pid, quant));
            id += 1;
        }

        // Create and save order
        EntityRef<OneOrder.Command> newOrder = sharding.entityRefFor(OneOrder.ENTITY_KEY, String.valueOf(orderId));
        OneOrder.Order order_obj = new OneOrder.Order(orderId, orderRequests.user_id, (int)totalCost,
                OrderStatus.PLACED, orderItems);
        newOrder.tell(new OneOrder.SetOrder(order_obj));

        // Send success response
        OrderPostResponse.OrderSuccess obj = new OrderPostResponse.OrderSuccess(
                orderId,
                orderRequests.user_id,
                (int)totalCost,
                OrderStatus.PLACED,
                orderItems
        );

        replyTo.tell(obj);
        prevActor.tell(new Gateway.OrderSuccess(orderId));
        System.out.println("Order placed successfully");

        // Self-terminate after completion
        getContext().getSelf().tell(new Stop());
    }

    private void releaseReservedStock() {
        for (OrderItemRequests item : orderRequests.items) {
            if (processingState.stockReservations.getOrDefault(item.product_id, false)) {
                EntityRef<Oneproduct.Command> productEntity =
                        sharding.entityRefFor(Oneproduct.ENTITY_KEY, String.valueOf(item.product_id));

                productEntity.tell(new Oneproduct.ReleaseReservation(item.quantity));
            }
        }
    }

    private Behavior<Command> stopWithFailure(String reason, StatusCode statusCode) {
        getContext().getLog().error("Order processing failed: {}", reason);
        if (replyTo != null) {
            replyTo.tell(new OrderPostResponse.OrderFailure(statusCode.intValue(), reason));
        }
        return Behaviors.stopped();
    }

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public CompletionStage<Boolean> validateUser(Integer userId) {
        String userServiceUrl = getContext().getSystem().settings().config().getString("my-app.routes.user-service");

        return http.singleRequest(HttpRequest.GET(userServiceUrl + userId))
                .thenCompose(response -> {
                    if (response.status().equals(StatusCodes.OK)) {
                        return Unmarshaller.entityToString().unmarshal(response.entity(), getContext().getSystem())
                                .thenApply(jsonString -> {
                                    try {
                                        JsonNode jsonNode = objectMapper.readTree(jsonString);
                                        discountStatus = jsonNode.get("discount_availed").asBoolean();
                                        return true;
                                    } catch (Exception e) {
                                        getContext().getLog().error("Error parsing user JSON: {}", e.getMessage());
                                        return false;
                                    }
                                });
                    }
                    return CompletableFuture.completedFuture(false);
                });
    }

    // Process payment (debit from wallet)
    private CompletionStage<Boolean> processPayment(Integer userId, double amount) {
        String walletServiceUrl = getContext().getSystem().settings().config().getString("my-app.routes.wallet-service");

        // Build JSON payload for the debit operation
        String jsonPayload = String.format("{\"action\": \"debit\", \"amount\": %d}", (int)amount);

        // Create HTTP PUT request
        HttpRequest walletRequest = HttpRequest.create()
                .withMethod(HttpMethods.PUT)
                .withUri(walletServiceUrl + userId)
                .withEntity(HttpEntities.create(ContentTypes.APPLICATION_JSON, jsonPayload));

        if(http == null){
            getContext().getLog().warn("!!!!!!!!!!http is null!!!!!!!!!!!");
        }

        // Send request
        return http.singleRequest(walletRequest).thenCompose(response -> {
            if (response.status().isSuccess()) {
                response.discardEntityBytes(getContext().getSystem());
                return CompletableFuture.completedFuture(true);
            } else {
                response.discardEntityBytes(getContext().getSystem());
                return CompletableFuture.completedFuture(false);
            }
        });
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Stop.class, msg -> {
                    getContext().getLog().info("Stopping OnePlaceOrder actor for Order ID: {}", orderId);
                    return Behaviors.stopped();
                })
                .onMessage(placeOrderPackage.class, this::onPlaceOrderPackage)
                .onMessage(StockReservationResponse.class, msg -> {
                    handleStockReservationResponse(msg);
                    return this;
                })
                .onMessage(ProductDetailsResponse.class, msg -> {
                    handleProductDetailsResponse(msg);
                    return this;
                })
                .onMessage(DiscountStatusResponse.class, msg -> {
                    handleDiscountStatusResponse(msg);
                    return this;
                })
                .onMessage(ApplyDiscountResponse.class, msg -> {
                    handleApplyDiscountResponse(msg);
                    return this;
                })
                .build();
    }

    public Behavior<Command> onPlaceOrderPackage(placeOrderPackage msg) {
        this.orderRequests = msg.orderRequests;
        this.replyTo = msg.replyTo;
        this.orderId = msg.orderId;
        this.prevActor = msg.orderMap;
        this.discountManagerRef = msg.discountManagerRef;

        return processOrder();
    }
}