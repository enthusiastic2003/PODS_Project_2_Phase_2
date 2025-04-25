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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class OnePlaceOrder extends AbstractBehavior<OnePlaceOrder.Command> {

    public interface Command {}
    public interface Response {}

    public static class Stop implements Command {} // Stop command

    private ClusterSharding sharding;

    private  OrderPostRequests orderRequests;
    public Integer portUserService = 8080;
    public Integer portWalletService = 8082;
    private  ActorRef<OrderPostResponse.Response> replyTo;
    private Boolean discountStatus = false;
    private ActorRef<Gateway.Command> prevActor;
    public  ActorRef<DiscountManager.Command> discountManagerRef;
    private Integer orderId;
    private  Http http;

    private OnePlaceOrder(ActorContext<Command> context){

        super(context);
        http = Http.get(context.getSystem());
        sharding = ClusterSharding.get(context.getSystem());

    }


    public static class placeOrderPackage extends SerializableTraitClass implements Command {

        public final OrderPostRequests orderRequests;
        public final Integer orderId;
        public final ActorRef<Gateway.Command> orderMap;

        // Transient fields since Akka Jackson cannot serialize ActorRef or ClusterSharding directly
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

            // Transient fields are set to null in deserialized version unless re-attached manually
            this.replyTo = replyTo;
            this.discountManagerRef = discountManagerRef;

        }
    }

    public static Behavior<Command> create() {
        return Behaviors.setup(context -> new OnePlaceOrder(context));
    }

    private Behavior<Command> processOrder() {

//        int port = getContext().getSystem().settings()
//                .config().getInt("akka.remote.artery.canonical.port");

        getContext().getLog().info("Processing order: {}", orderRequests);

        getContext().getLog().info("ARG PORT: " + getContext().getSystem().settings().config().getInt("akka.remote.artery.canonical.port"));

        if(http == null){
            getContext().getLog().info("No HTTP connection");
        }
        else{
            getContext().getLog().info(http.toString());
        }

        // Step 1: Validate user
        if (!validateUser(orderRequests.user_id).toCompletableFuture().join()) {
            return stopWithFailure("User validation failed", StatusCodes.BAD_REQUEST);
        }
        CompletionStage<DiscountManager.GetDiscountResponse> discountStatusActual =
                AskPattern.ask(
                        discountManagerRef,
                        (ActorRef<DiscountManager.GetDiscountResponse> replyTo) ->
                                new DiscountManager.GetDiscountStatus(orderRequests.user_id, discountStatus , replyTo),
                        Duration.ofSeconds(3),
                        getContext().getSystem().scheduler()
                );

        discountStatus = discountStatusActual.toCompletableFuture().join().discount_availed;

        System.out.println("User validated");

        // Step 2: Check product availability and reserves stock atomically
        // This new method will check availability and reserve the stock in one atomic operation
        CompletionStage<Boolean> stockReservationResult = reserveStock(orderRequests);
        if (!stockReservationResult.toCompletableFuture().join()) {
            return stopWithFailure("Product not found or stock insufficient", StatusCodes.BAD_REQUEST);
        }
        System.out.println("Stock reserved successfully");

        // Step 3: Calculate total cost and process payment
        CompletionStage<Double> totalCostResult = calculateTotalCost(orderRequests);
        double totalCost = totalCostResult.toCompletableFuture().join();

        // Apply discount if applicable
        CompletionStage<DiscountManager.GetDiscountResponse> discountStatusBeforeApply =
                AskPattern.ask(
                        discountManagerRef,
                        (ActorRef<DiscountManager.GetDiscountResponse> replyTo) ->
                                new DiscountManager.ApplyDiscount(orderRequests.user_id , replyTo),
                        Duration.ofSeconds(3),
                        getContext().getSystem().scheduler()
                );

        discountStatus = discountStatusBeforeApply.toCompletableFuture().join().discount_availed;
        if (!discountStatus) {
            totalCost = totalCost * 0.90;
        }

        // Process payment
        if (!processPayment(orderRequests.user_id, totalCost).toCompletableFuture().join()) {
            // If payment fails, release the reserved stock
            releaseReservedStock(orderRequests);
            discountManagerRef.tell(new DiscountManager.RevertDiscountLock(orderRequests.user_id));
            return stopWithFailure("Payment processing failed", StatusCodes.BAD_REQUEST);
        }

        System.out.println("Payment processed successfully");

        if(!discountStatus){

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

        //orderMap.put(orderId, order_obj);


        // Send success response
        OrderPostResponse.OrderSuccess obj = new OrderPostResponse.OrderSuccess(
                orderId,
                orderRequests.user_id,
                (int)totalCost,
                OrderStatus.PLACED,
                orderItems
        );

        replyTo.tell(obj);
        prevActor.tell(
                new Gateway.OrderSuccess(orderId)
        );
        System.out.println("Order placed successfully");
        return Behaviors.stopped();
    }

    private Behavior<Command> stopWithFailure(String reason, StatusCode statusCode) {
        getContext().getLog().error("Order processing failed: {}", reason);
        replyTo.tell(new OrderPostResponse.OrderFailure(statusCode, reason));
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

    // New method to reserve stock atomically
    private CompletionStage<Boolean> reserveStock(OrderPostRequests request) {
        CompletableFuture<Boolean> result = CompletableFuture.completedFuture(true);

        // Process each item sequentially using CompletableFuture chaining
        for (OrderItemRequests item : request.items) {
            int pid = item.product_id;
            int quantity = item.quantity;

            // Skip if product doesn't exist
            if (!((pid>=101)&&(pid<=120))) {
                result = result.thenApply(success -> false);
                continue;
            }

            // Get product entity ref
            EntityRef<Oneproduct.Command> productEntity =
                    sharding.entityRefFor(Oneproduct.ENTITY_KEY, String.valueOf(pid));

            // Chain the reservation request
            result = result.thenCompose(success -> {
                if (!success) return CompletableFuture.completedFuture(false);

                // Use the actor's "ask" pattern to try to reserve stock atomically
                return AskPattern.<Oneproduct.Command, ProdResponses.ReservationResponse>ask(
                        productEntity,
                        replyTo -> new Oneproduct.ReserveStock(quantity, replyTo), // You'll need to add this command
                        Duration.ofSeconds(3),
                        getContext().getSystem().scheduler()
                ).thenApply(response -> {
                    if (response instanceof ProdResponses.ReservationSuccess) {
                        return true;
                    } else {
                        return false;
                    }
                });
            });
        }

        return result;
    }

    // Method to release reserved stock if payment fails
    private void releaseReservedStock(OrderPostRequests request) {
        for (OrderItemRequests item : request.items) {
            EntityRef<Oneproduct.Command> productEntity =
                    sharding.entityRefFor(Oneproduct.ENTITY_KEY, String.valueOf(item.product_id));

            productEntity.tell(new Oneproduct.ReleaseReservation(item.quantity));
        }
    }

    // Calculate total cost without debiting
    private CompletionStage<Double> calculateTotalCost(OrderPostRequests request) {
        double totalCost = 0;
        CompletableFuture<Double> result = CompletableFuture.completedFuture(totalCost);

        for (OrderItemRequests item : request.items) {
            int pid = item.product_id;
            int quantity = item.quantity;

            // Chain the product detail requests
            result = result.thenCompose(currentTotal -> {
                EntityRef<Oneproduct.Command> productEntity =
                        sharding.entityRefFor(Oneproduct.ENTITY_KEY, String.valueOf(pid));

                return AskPattern.<Oneproduct.Command, ProductResponse>ask(
                        productEntity,
                        replyTo -> new Oneproduct.GetProductDetails(replyTo),
                        Duration.ofSeconds(3),
                        getContext().getSystem().scheduler()
                ).thenApply(response -> {
                    if (response instanceof ProductFound) {
                        ProductFound found = (ProductFound) response;
                        return currentTotal + (found.product.price * quantity);
                    } else {
                        return currentTotal;
                    }
                });
            });
        }

        return result;
    }

    // Process payment (debit from wallet)
    private CompletionStage<Boolean> processPayment(Integer userId, double amount) {
        String walletServiceUrl =  getContext().getSystem().settings().config().getString("my-app.routes.wallet-service");


        // Build JSON payload for the debit operation
        String jsonPayload = String.format("{\"action\": \"debit\", \"amount\": %d}", (int)amount);

        // Create HTTP PUT request
        HttpRequest walletRequest = HttpRequest.create()
                .withMethod(HttpMethods.PUT)
                .withUri(walletServiceUrl + userId)
                .withEntity(HttpEntities.create(ContentTypes.APPLICATION_JSON, jsonPayload));

        if(http == null){
            getContext().getLog().warn("!!!!!!!!!!http is  null!!!!!!!!!!!");
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