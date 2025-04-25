package com.example.ImportantActors;

import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.example.SerializableTraitClass;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * ProdResponses actor handles response messages related to product operations.
 * It acts as a container for response message types and provides handling for response messages.
 * This class is used in conjunction with the Oneproduct actor for inventory management operations.
 */
public class ProdResponses extends AbstractBehavior<ProdResponses.Response> {

    /**
     * Constructor for the ProdResponses actor.
     *
     * @param context The actor context provided by Akka.
     */
    public ProdResponses(ActorContext<Response> context) {
        super(context);
    }

    /**
     * Interface defining reservation response messages.
     * This marker interface is implemented by success and failure response classes.
     */
    public interface ReservationResponse  {}

    /**
     * Response message for successful stock reservation.
     */
    public static class ReservationSuccess extends SerializableTraitClass implements ReservationResponse {
        public final int remainingStock;

        @JsonCreator
        public ReservationSuccess(@JsonProperty("remainingStock") int remainingStock) {
            this.remainingStock = remainingStock;
        }
    }

    /**
     * Response message for failed stock reservation.
     */
    public static class ReservationFailure extends SerializableTraitClass  implements ReservationResponse {
        public final String reason;

        @JsonCreator
        public ReservationFailure(@JsonProperty("reason") String reason) {
            this.reason = reason;
        }
    }

    /**
     * Base interface for all response messages.
     */
    public interface Response {}

    /**
     * Response message for stock addition operations.
     */
    public static class AddStatus extends SerializableTraitClass  implements Response {
        public final boolean addstatus;

        @JsonCreator
        public AddStatus(@JsonProperty("addstatus") boolean addstatus) {
            this.addstatus = addstatus;
        }
    }

    /**
     * Response message for stock subtraction operations.
     */
    public static class SubtractStatus extends SerializableTraitClass  implements Response {
        public final boolean subtractstatus;

        @JsonCreator
        public SubtractStatus(@JsonProperty("subtractstatus") boolean subtractstatus) {
            this.subtractstatus = subtractstatus;
        }
    }

    /**
     * Defines how the actor responds to different response messages.
     */
    public Receive<Response> createReceive() {
        return newReceiveBuilder()
                .onMessage(AddStatus.class, msg -> Behaviors.stopped())
                .onMessage(SubtractStatus.class, msg -> Behaviors.stopped())
                .build();
    }
}
