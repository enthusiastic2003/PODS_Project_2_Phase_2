package com.example;

import akka.actor.typed.ActorSystem;
import com.example.Gateway.Gateway;

public class QuickstartApp {
    public static void main(String[] args) {
        // Create the Gateway actor system
        ActorSystem<Gateway.Command> system = ActorSystem.create(Gateway.create(), "GatewaySystem");

        // Send the Initialize message to start the HTTP server
        system.tell(new Gateway.Initialize());

        // Log system startup
        System.out.println("Gateway system started...");
    }
}
