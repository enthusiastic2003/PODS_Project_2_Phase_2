package com.example.ImportantActors;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.Behaviors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

public class Dummy extends AbstractBehavior<Dummy.Command> {
    private int dummyid;
    // Define the Command interface
    public interface Command {
    }
    // Add a static factory method for creating the Dummy actor
    public static Behavior<Command> create() {
        return Behaviors.setup(Dummy::new);
    }
    // Constructor
    public Dummy(ActorContext<Command> context) {
        super(context);
        this.dummyid = 0; // Initialize with a default value, update as needed
    }
    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
        .onMessage(Print.class, this::onPrint)
        .build();

    }
    // Define the Print class that implements Command
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
    public static class Print implements Command {
        @JsonCreator
        public Print() {}
    }
    // Define the onPrint method
    private Behavior<Command> onPrint(Print command) {
        getContext().getLog().info("dummyid: {}", dummyid);
        return this;
    }
}