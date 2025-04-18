package com.example.Gateway;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.server.Route;

import com.example.ImportantActors.*;
import com.example.Requests.OrderPostRequests;
import com.example.Responses.*;
import com.example.Routers.ProductRoutes;
import com.example.Routers.OrderRoutes;
import com.typesafe.config.Config;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

    // GET /products/{productId} – forward the request to the corresponding product actor.
    public static final class GetProduct implements Command {
        public final int productId;
        public final ActorRef<ProductResponse> replyTo;
        public GetProduct(int productId, ActorRef<ProductResponse> replyTo) {
            this.productId = productId;
            this.replyTo = replyTo;
        }
    }

    // GET /orders/{orderId} – forward the request to the corresponding order actor.
    public static final class GetOrder implements Command {
        public final int orderId;
        public final ActorRef<OrderGetResponse.Response> replyTo;
        public GetOrder(int orderId, ActorRef<OrderGetResponse.Response> replyTo) {
            this.orderId = orderId;
            this.replyTo = replyTo;
        }
    }

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
    public static class DeleteOrder implements Command {
        public final Integer order_id;
        public final ActorRef<OrderDelete.Response> replyTo;
        public DeleteOrder(int order_id, ActorRef<OrderDelete.Response> replyTo) {
            this.order_id = order_id;
            this.replyTo = replyTo;
        }

    }

    // ----- Internal State -----
    private ServerBinding serverBinding;
    public ClusterSharding sharding = ClusterSharding.get(getContext().getSystem());
    public ActorRef<DiscountManager.Command> discountManagerRef;
    // Constructor: loads products from CSV and registers each as a cluster-sharded entity.
    // (Assumes that each product actor is created via Oneproduct.create and uses its ENTITY_KEY.)



    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Initialize.class, this::onInitialize)
                .onMessage(GetProduct.class, this::onGetProduct)
                .onMessage(GetOrder.class, this::onGetOrder)
                .onMessage(PutOrderStatus.class, this::onPutOrderStatus)
                .onMessage(PlaceOrder.class, this::onPlaceOrder)
                .onMessage(DeleteOrder.class, this::onDeleteOrder)
                .build();
    }

    // ----- Message Handlers -----

    // Handler for initialization message
    private Behavior<Command> onInitialize(Initialize msg) {
        getContext().getLog().info("Initializing HTTP server...");
        startHttpServer();  // Start the HTTP server for external API calls
        return this;  // Maintain current behavior
    }

    // Handler for product lookup requests
    private Behavior<Command> onGetProduct(GetProduct msg) {
        int productId = msg.productId;

        // Check if product exists in local registry
        if (productMap.containsKey(productId)) {
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
        
        // Create child actor to handle order processing
        ActorRef<OnePlaceOrder.Command> orderProcessor = getContext().spawn(
            OnePlaceOrder.create(
                msg, 
                latestOrderId, 
                sharding,
                productMap, 
                orderMap, 
                discountManagerRef
            ),
            "OnePlaceOrder_" + latestOrderId  // Unique name for child actor
        );

        getContext().getLog().info("Spawned OnePlaceOrder Actor for Order ID: {}", latestOrderId);
        
        return this;
    }

    // Handler for order retrieval requests
    private Behavior<Command> onGetOrder(GetOrder msg) {
        if (orderMap.containsKey(msg.orderId)) {
            // Get sharded entity reference for the order
            EntityRef<OneOrder.Command> orderEntity = sharding.entityRefFor(
                OneOrder.ENTITY_KEY, 
                String.valueOf(msg.orderId)
            );
            
            // Async ask pattern to get order details
            CompletionStage<OneOrder.Order> result = AskPattern.ask(
                orderEntity,
                replyTo -> new OneOrder.GetOrderDetails(replyTo),
                Duration.ofSeconds(3),  // Timeout for safety
                getContext().getSystem().scheduler()
            );

            // Blocking wait for result (anti-pattern in reactive systems)
            OneOrder.Order order = result.toCompletableFuture().join();
            msg.replyTo.tell(new OrderGetResponse.OrderSuccess(order));
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
            msg.replyTo.tell(new OrderPutResponse(StatusCodes.BAD_REQUEST, "Order Not Found"));
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

        // Create child actor to handle deletion process
        ActorRef<OneDeleteOrder.Command> deleteOrderActor = getContext().spawn(
            OneDeleteOrder.create(msg, sharding, productMap),
            "DeleteOrderActor_" + uniqdeleteid  // Unique actor name
        );
        uniqdeleteid++;
        
        // Note: The actual delete command to the order entity is commented out
        // deleteOrderActor.tell(new OneOrder.DeleteOrder(msg.order_id, msg.replyTo));
        
        return this;
    }


    // ----- Helper Methods -----
    // In your main actor or service class
    public static Behavior<Command> create() {
        return Behaviors.setup(Gateway::new);
    }

    private Gateway(ActorContext<Command> context) {
        super(context);

        this.discountManagerRef = getContext().spawn(DiscountManager.create(), "discountManager");



        // Initialize the ClusterSharding extension
        this.sharding = ClusterSharding.get(context.getSystem());

        // Load products from CSV
        this.productMap = loadProducts(context);

        // Initialize sharding for products
        this.sharding.init(
                Entity.of(
                        Oneproduct.ENTITY_KEY, entityContext ->
                        //        Oneproduct.create(Integer.parseInt(entityContext.getEntityId()))
                        {
                            int productId = Integer.parseInt(entityContext.getEntityId());
                            Oneproduct.Product product = productMap.get(productId);

                            if (product == null) {
                                context.getLog().error("Product with ID {} not found", productId);
                                return Behaviors.empty();
                            }

                            return Oneproduct.create(product);
                        }
                )
        );

        this.sharding.init(
                Entity.of(
                        OneOrder.ENTITY_KEY, entityContext -> OneOrder.create()
                )
        );



        context.getLog().info("Gateway initialized with {} products and sharding configured", productMap.size());
    }


    private static Map<Integer, Oneproduct.Product> loadProducts(ActorContext<?> context) {
        Map<Integer, Oneproduct.Product> productMap = new HashMap<>();
        try {
            // Assumes "products.csv" is in the resources folder
            Path path = Paths.get(Gateway.class.getClassLoader().getResource("products.csv").toURI());
            List<String> lines = Files.readAllLines(path);
            if (!lines.isEmpty()) {
                // Remove header line
                lines.remove(0);
            }

            context.getLog().info("Loading {} products", lines.size());

            for (String line : lines) {
                String[] parts = line.split(",");
                int id = Integer.parseInt(parts[0]);
                String name = parts[1];
                String description = parts[2];
                int price = Integer.parseInt(parts[3]);
                int stockQuantity = Integer.parseInt(parts[4]);

                Oneproduct.Product product = new Oneproduct.Product(id, name, description, price, stockQuantity);
                productMap.put(id, product);
            }

            context.getLog().info("Successfully loaded {} products from CSV", productMap.size());
        } catch (Exception e) {
            context.getLog().error("Failed to load products from CSV", e);
        }
        return productMap;
    }
    // In Gateway.java
    private void startHttpServer() {
        // Retrieve the port from the configuration
        Config config = getContext().getSystem().settings().config();

        // Read your specific value
        int port = config.getInt("my-app.routes.myport");

        // Instantiate routes. Modify as needed for your application's routing.
        ProductRoutes productRoutes = new ProductRoutes(getContext().getSystem(), getContext().getSelf());
        OrderRoutes orderRoutes = new OrderRoutes(getContext().getSystem(), getContext().getSelf());

        // Combine the routes into a single route
        Route combinedRoutes = concat(productRoutes.productRoutes(), orderRoutes.orderRoutes());

        // Convert the Akka Typed system to Classic system to use Akka HTTP
        akka.actor.ActorSystem classicSystem = Adapter.toClassic(getContext().getSystem());

        // Bind to the port specified in the configuration
        CompletionStage<ServerBinding> futureBinding =
                Http.get(classicSystem).newServerAt("0.0.0.0", port).bind(combinedRoutes);

        futureBinding.whenComplete((binding, exception) -> {
            if (binding != null) {
                serverBinding = binding;
                InetSocketAddress address = binding.localAddress();
                getContext().getLog().info("HTTP server online at http://{}:{}/",
                        address.getHostString(), address.getPort());
            } else {
                getContext().getLog().error("Failed to bind HTTP endpoint", exception);
                classicSystem.terminate();
            }
        });
    }



}
