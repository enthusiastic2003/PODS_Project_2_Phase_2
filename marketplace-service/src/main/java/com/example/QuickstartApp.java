package com.example;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.receptionist.ServiceKey;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.javadsl.Routers;

import com.example.Gateway.Gateway;
import com.example.ImportantActors.OnePlaceOrder;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Config;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.GroupRouter;
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.typed.Cluster;


import java.util.HashMap;
import java.util.Map;

public class QuickstartApp {

  
    public static void main(String[] args) {
        // print args
        System.out.println("Arguments: " + String.join(", ", args));

                
        // Default port
        int port = 25251;
        // Check if port is passed as an argument
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port argument, using default port: " + port);
            }
        }

        // Override configuration
        Map<String, Object> overrides = new HashMap<>();
        overrides.put("akka.remote.artery.canonical.port", port);
        Config config = ConfigFactory.parseMap(overrides).withFallback(ConfigFactory.load());

        // Print the configured port to verify
        System.out.println("Configured akka.remote.artery.canonical.port: " + config.getString("akka.remote.artery.canonical.port"));

       

        if(Integer.parseInt(args[0]) == 8083){
            System.out.println("Given port number is 8083. ie. it is primary node");
            // Create the Gateway actor system with the overridden configuration
            ActorSystem<Gateway.Command> system = ActorSystem.create(
                Gateway.create(),
                "GatewaySystem",
                config
            );
            // Send the Initialize message to start the HTTP server
            system.tell(new Gateway.Initialize());
            // Log system startup
            System.out.println("Gateway system started...");

        } else {
            System.out.println("Port is not 8083");
        }

        




        
    }
    
}
