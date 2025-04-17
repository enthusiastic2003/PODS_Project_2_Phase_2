package com.example.ImportantActors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.example.ImportantActors.Oneproduct;

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
    public interface ReservationResponse {}

    /**
     * Response message for successful stock reservation.
     * Sent when a product's stock has been successfully reserved for an order.
     */
    public static class ReservationSuccess implements ReservationResponse {
        public final int remainingStock;  // The amount of stock remaining after reservation

        public ReservationSuccess(int remainingStock) {
            this.remainingStock = remainingStock;
        }
    }

    /**
     * Response message for failed stock reservation.
     * Sent when a reservation request cannot be fulfilled (insufficient stock, product not found, etc.).
     */
    public static class ReservationFailure implements ReservationResponse {
        public final String reason;  // Description of why the reservation failed

        public ReservationFailure(String reason) {
            this.reason = reason;
        }
    }

    /**
     * Base interface for all response messages that this actor can handle.
     * All response types must implement this marker interface.
     */
    public interface Response {}

    /**
     * Response message for stock addition operations.
     * Sent after an attempt to add stock to a product.
     */
    public static class AddStatus implements Response {
        public final boolean addstatus;  // True if stock was successfully added

        public AddStatus(boolean addstatus) {
            this.addstatus = addstatus;
        }
    }

    /**
     * Response message for stock subtraction operations.
     * Sent after an attempt to subtract stock from a product.
     */
    public static class SubtractStatus implements Response {
        public final boolean subtractstatus;  // True if stock was successfully subtracted

        public SubtractStatus(boolean subtractstatus) {
            this.subtractstatus = subtractstatus;
        }
    }

    /**
     * Defines how the actor responds to different response messages.
     * Currently, this actor simply stops itself after receiving any message.
     * This pattern is typically used for temporary actors that are created
     * just to receive a single response.
     *
     * @return The behavior definition for handling messages
     */
    public Receive<ProdResponses.Response> createReceive() {
        return newReceiveBuilder()
                .onMessage(AddStatus.class, msg -> {
                    // After receiving an AddStatus message, stop the actor
                    return Behaviors.stopped();
                })
                .onMessage(SubtractStatus.class, msg -> {
                    // After receiving a SubtractStatus message, stop the actor
                    return Behaviors.stopped();
                })
                .build();
    }
}