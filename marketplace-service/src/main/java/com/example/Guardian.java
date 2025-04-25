package com.example;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.cluster.typed.Cluster;
import akka.cluster.typed.ClusterSingleton;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.server.Route;
import com.example.Gateway.Gateway;
import com.example.ImportantActors.OneOrder;
import com.example.ImportantActors.OnePlaceOrder;
import com.example.ImportantActors.Oneproduct;
import com.example.Routers.OrderRoutes;
import com.example.Routers.ProductRoutes;
import com.typesafe.config.Config;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import static akka.http.javadsl.server.Directives.concat;

public class Guardian {

    private static ServerBinding serverBinding;

    public static Behavior<Void> create(int port) {
        return Behaviors.setup(context -> {

            ActorRef<Oneproduct.Ack> ignoreReplies = context.spawn(
                    Behaviors.receive(Oneproduct.Ack.class)
                            .onAnyMessage(msg -> Behaviors.same())
                            .build(),
                    "IgnorePingAcks"
            );


            // Create ServiceKey for OnePlaceOrder actors
            ServiceKey<OnePlaceOrder.Command> serviceKey = ServiceKey.create(OnePlaceOrder.Command.class, "TempActorServ");

            // Register 50 PlaceOrder actors in the Receptionist
//            for (int i = 1; i <= 50; i++) {
//                ActorRef<OnePlaceOrder.Command> ref = context.spawn(OnePlaceOrder.create(), "PlaceOrder" + i);
//                context.getSystem().receptionist().tell(Receptionist.register(serviceKey, ref));
//            }

            // If the node is the primary (port 8083), start the Gateway and HTTP server
            if (port == 8083) {

                // Start HTTP server with the gateway reference

                ClusterSharding shardingF1 = ClusterSharding.get(context.getSystem());

                // Read from csv
                Map<Integer, Oneproduct.Product> products = loadProducts(context, 101, 110);

                shardingF1.init(
                        Entity.of(
                                Oneproduct.ENTITY_KEY, entityContext -> Behaviors.setup(actorContext -> {
                                    int productId = Integer.parseInt(entityContext.getEntityId());
                                    Oneproduct.Product product = products.get(productId);

                                    if (product == null) {
                                        actorContext.getLog().error("Product with ID {} not found", productId);
                                        return Behaviors.empty(); // Or Behaviors.stopped() if no further processing is needed
                                    }

                                    // Return the behavior for this specific product
                                    return Oneproduct.create(product);
                                })
                        )
                );

                for(Integer productId : products.keySet()) {
                    EntityRef<Oneproduct.Command> productRef = shardingF1.entityRefFor(Oneproduct.ENTITY_KEY, String.valueOf(productId));
                    context.getLog().info("Product with ID {} found", productId);
                    productRef.tell(new Oneproduct.Ping(ignoreReplies));
                    // or create a temporary actor to receive Ack
                }

                shardingF1.init(
                        Entity.of(OneOrder.ENTITY_KEY, entityContext ->
                                OneOrder.create()));

                //Init Gateway actor
                ActorRef<Gateway.Command> gatewayRef = context.spawn(Gateway.create(serviceKey), "Gateway");

                startHttpServer(context, gatewayRef);

            }

            if(port == 8084){

                for (int i = 1; i <= 50; i++) {
                    ActorRef<OnePlaceOrder.Command> ref = context.spawn(OnePlaceOrder.create(), "PlaceOrder" + i);
                    context.getSystem().receptionist().tell(Receptionist.register(serviceKey, ref));
                }

                Map<Integer, Oneproduct.Product> products2 = loadProducts(context, 111, 120);

                ClusterSharding shardingF2 = ClusterSharding.get(context.getSystem());

                shardingF2.init(
                        Entity.of(OneOrder.ENTITY_KEY, entityContext ->
                                OneOrder.create()));

                shardingF2.init(
                        Entity.of(
                                Oneproduct.ENTITY_KEY, entityContext -> {
                                    int productId = Integer.parseInt(entityContext.getEntityId());
                                    Oneproduct.Product product = products2.get(productId);

                                    if (product == null) {
                                        context.getLog().warn("Product with ID {} not found on 8084", productId);
                                        return Behaviors.empty();
                                    }

                                    return Oneproduct.create(product);
                                }
                        )
                );

                for(Integer productId : products2.keySet()) {
                    EntityRef<Oneproduct.Command> productRef = shardingF2.entityRefFor(Oneproduct.ENTITY_KEY, String.valueOf(productId));
                    context.getLog().info("Product with ID {} found", productId);
                    productRef.tell(new Oneproduct.Ping(ignoreReplies));
                }


            }



            return Behaviors.empty();
        });
    }

    private static Map<Integer, Oneproduct.Product> loadProducts(ActorContext<?> context, Integer from, Integer to) {
        Map<Integer, Oneproduct.Product> productMap = new HashMap<>();
        try {
            // Assumes "products.csv" is in the resources folder
            Path path = Paths.get(Gateway.class.getClassLoader().getResource("products.csv").toURI());
            List<String> lines = Files.readAllLines(path);
            if (!lines.isEmpty()) {
                // Remove header line
                lines.remove(0);
            }

            context.getLog().info("Loading products with ID from {} to {}", from, to);

            for (String line : lines) {
                String[] parts = line.split(",");
                int id = Integer.parseInt(parts[0]);
                if (id < from || id > to) continue; // Only include IDs in the range

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


    private static void startHttpServer(ActorContext<Void> context, ActorRef<Gateway.Command> gatewayRef) {
        // Retrieve the port from the configuration
        Config config = context.getSystem().settings().config();

        // Read your specific value from the config
        int port = config.getInt("my-app.routes.myport");

        // Instantiate routes with the gateway reference
        ProductRoutes productRoutes = new ProductRoutes(context.getSystem(), gatewayRef);
        OrderRoutes orderRoutes = new OrderRoutes(context.getSystem(), gatewayRef);

        // Combine routes into a single route
        Route combinedRoutes = concat(productRoutes.productRoutes(), orderRoutes.orderRoutes());

        // Get the classic ActorSystem from the typed ActorSystem
        akka.actor.ActorSystem classicSystem = akka.actor.typed.javadsl.Adapter.toClassic(context.getSystem());

        // Bind to the specified port
        CompletionStage<ServerBinding> futureBinding =
                Http.get(classicSystem).newServerAt("0.0.0.0", port).bind(combinedRoutes);

        futureBinding.whenComplete((binding, exception) -> {
            if (binding != null) {
                serverBinding = binding;
                InetSocketAddress address = binding.localAddress();
                context.getLog().info("HTTP server online at http://{}:{}/",
                        address.getHostString(), address.getPort());
            } else {
                context.getLog().error("Failed to bind HTTP endpoint", exception);
                context.getSystem().terminate();
            }
        });
    }
}