package com.example.ImportantActors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.serialization.jackson.JsonSerializable;
import com.example.Responses.ProductFound;
import com.example.Responses.ProductResponse;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The Oneproduct actor represents a single product in an inventory system.
 * It handles operations like checking product details, managing stock levels,
 * and reserving inventory for orders.
 *
 * This actor is designed to work within an Akka Cluster Sharding context,
 * where each product instance is a separate actor identified by its product ID.
 */
public class Oneproduct extends AbstractBehavior<Oneproduct.Command> {

    public static final EntityTypeKey<Oneproduct.Command> ENTITY_KEY =
            EntityTypeKey.create(Oneproduct.Command.class, "OneProduct");

    /**
     * Internal data structure that holds product information.
     * Note that stock_quantity is mutable while other properties are immutable.
     */

    public static class Product implements JsonSerializable {
        @JsonProperty("id")
        public final Integer id;
        @JsonProperty("name")
        public final String name;
        @JsonProperty("description")
        public final String description;
        @JsonProperty("price")
        public final Integer price;
        @JsonProperty("stock_quantity")
        public Integer stock_quantity; // mutable

        @JsonCreator
        public Product(
                @JsonProperty("id") Integer id,
                @JsonProperty("name") String name,
                @JsonProperty("description") String description,
                @JsonProperty("price") Integer price,
                @JsonProperty("stock_quantity") Integer stock_quantity
        ) {
            this.id = id;
            this.name = name;
            this.description = description;
            this.price = price;
            this.stock_quantity = stock_quantity;
        }
    }

    /**
     * Base interface for all commands that this actor can handle.
     * All command types must implement this marker interface.
     */
    public interface Command extends JsonSerializable { }

    /**
     * Command to reserve a specific quantity of stock for an order being processed.
     * This is typically used in the first step of order processing before confirming payment.
     */
    public static class ReserveStock implements Command {
        @JsonProperty("quantity")
        public final int quantity;                                      // Quantity to reserve
        @JsonProperty("replyTo")
        public final ActorRef<ProdResponses.ReservationResponse> replyTo;  // Actor to send the response to

        @JsonCreator
        public ReserveStock(@JsonProperty("quantity") int quantity,
                            @JsonProperty("replyTo") ActorRef<ProdResponses.ReservationResponse> replyTo) {
            this.quantity = quantity;
            this.replyTo = replyTo;
        }
    }

    /**
     * New Tell-based command to reserve stock
     */
    public static class ReserveStockTell implements Command {
        @JsonProperty("quantity")
        public final int quantity;                                   // Quantity to reserve
        @JsonProperty("productId")
        public final int productId;                                  // Product ID for response identification
        @JsonProperty("replyTo")
        public final ActorRef<OnePlaceOrder.Command> replyTo;        // Actor to tell the response to

        @JsonCreator
        public ReserveStockTell(
                @JsonProperty("quantity") int quantity,
                @JsonProperty("productId") int productId,
                @JsonProperty("replyTo") ActorRef<OnePlaceOrder.Command> replyTo) {
            this.quantity = quantity;
            this.productId = productId;
            this.replyTo = replyTo;
        }
    }

    /**
     * New Tell-based command to get product details
     */
    public static class GetProductDetailsTell implements Command {
        @JsonProperty("productId")
        public final int productId;                                 // Product ID for response identification
        @JsonProperty("replyTo")
        public final ActorRef<OnePlaceOrder.Command> replyTo;       // Actor to tell the response to

        @JsonCreator
        public GetProductDetailsTell(
                @JsonProperty("productId") int productId,
                @JsonProperty("replyTo") ActorRef<OnePlaceOrder.Command> replyTo) {
            this.productId = productId;
            this.replyTo = replyTo;
        }
    }

    /**
     * Command to release previously reserved stock, for example when an order is canceled.
     * This makes the previously reserved quantity available for other orders.
     */
    public static class ReleaseReservation implements Command {
        @JsonProperty("quantity")
        public final int quantity;    // Quantity to release back to available stock

        @JsonCreator
        public ReleaseReservation(@JsonProperty("quantity") int quantity) {
            this.quantity = quantity;
        }
    }

    /**
     * Command to add units to the product's stock level.
     * Used when new inventory arrives or for replenishment.
     */
    public static class AddToStock implements Command {
        @JsonProperty("add_amount")
        public final int add_amount;                              // Amount to add to stock
        @JsonProperty("replyTo")
        public final ActorRef<ProdResponses.AddStatus> replyTo;   // Actor to send response to

        @JsonCreator
        public AddToStock(@JsonProperty("add_amount") int add_amount,
                          @JsonProperty("replyTo") ActorRef<ProdResponses.AddStatus> replyTo) {
            this.add_amount = add_amount;
            this.replyTo = replyTo;
        }
    }

    /**
     * Special command to add stock when handling deleted orders.
     * This allows for direct communication with the OneDeleteOrder actor.
     */
    public static class AddToStockForDeleteOrder implements Command {
        @JsonProperty("add_amount")
        public final Integer add_amount;                     // Amount to add back to stock
        @JsonProperty("replyTo")
        public final ActorRef<OneDeleteOrder.Command> replyTo;  // DeleteOrder actor to notify

        @JsonCreator
        public AddToStockForDeleteOrder(@JsonProperty("add_amount") Integer add_amount,
                                        @JsonProperty("replyTo") ActorRef<OneDeleteOrder.Command> replyTo) {
            this.add_amount = add_amount;
            this.replyTo = replyTo;
        }
    }

    /**
     * Command to reduce the product's stock level.
     * Used when finalizing orders or removing damaged inventory.
     */
    public static class SubtractFromStock implements Command {
        @JsonProperty("subtract_amount")
        public final int subtract_amount;                              // Amount to remove from stock
        @JsonProperty("replyTo")
        public final ActorRef<ProdResponses.SubtractStatus> replyTo;   // Actor to send response to

        @JsonCreator
        public SubtractFromStock(@JsonProperty("subtract_amount") int subtract_amount,
                                 @JsonProperty("replyTo") ActorRef<ProdResponses.SubtractStatus> replyTo) {
            this.subtract_amount = subtract_amount;
            this.replyTo = replyTo;
        }
    }

    /**
     * Command to retrieve the full product details.
     * Used by UI components or other actors that need product information.
     */
    public static class GetProductDetails implements Command {
        @JsonProperty("replyTo")
        public final ActorRef<ProductResponse> replyTo;

        @JsonCreator
        public GetProductDetails(@JsonProperty("replyTo") ActorRef<ProductResponse> replyTo) {
            this.replyTo = replyTo;
        }
    }

    // Internal state - the product this actor instance represents
    private Product product;

    /**
     * Private constructor used with the factory method.
     * Initializes the actor with a Product instance.
     */
    private Oneproduct(ActorContext<Command> context, Product product) {
        super(context);
        this.product = product;
    }

    /**
     * Factory method to create a new Oneproduct actor behavior.
     * This is the recommended way to instantiate Akka actors.
     */
    public static Behavior<Command> create(Product product) {
        return Behaviors.setup(context -> new Oneproduct(context, product));
    }

    public static final class Ping implements Command {
        @JsonProperty("replyTo")
        public final ActorRef<Ack> replyTo;

        @JsonCreator
        public Ping(@JsonProperty("replyTo") ActorRef<Ack> replyTo) {
            this.replyTo = replyTo;
        }
    }

    public static final class Ack {}

    /**
     * Defines how the actor responds to different command messages.
     * This is the core message-handling logic of the actor.
     */
    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(AddToStock.class, this::onAddToStock)
                .onMessage(AddToStockForDeleteOrder.class, this::onAddToStockForDeleteOrder)
                .onMessage(SubtractFromStock.class, this::onSubtractFromStock)
                .onMessage(GetProductDetails.class, this::onGetProductDetails)
                .onMessage(ReserveStock.class, cmd -> {
                    Product product = this.product;
                    if (product == null) {
                        // Fail if product doesn't exist
                        cmd.replyTo.tell(new ProdResponses.ReservationFailure("Product not found"));
                        return this;
                    }

                    if (product.stock_quantity >= cmd.quantity) {
                        // If we have enough stock, decrease it and notify success
                        product.stock_quantity -= cmd.quantity;
                        cmd.replyTo.tell(new ProdResponses.ReservationSuccess(product.stock_quantity));
                        return this;
                    } else {
                        // Not enough stock available
                        cmd.replyTo.tell(new ProdResponses.ReservationFailure("Insufficient stock"));
                        return this;
                    }
                })
                .onMessage(ReserveStockTell.class, this::onReserveStockTell)
                .onMessage(GetProductDetailsTell.class, this::onGetProductDetailsTell)
                .onMessage(ReleaseReservation.class, cmd -> {
                    Product product = this.product;
                    if (product != null) {
                        // Return the reserved quantity back to available stock
                        product.stock_quantity += cmd.quantity;
                    }
                    return this;
                })
                .onMessage(Ping.class, msg -> {
                    msg.replyTo.tell(new Ack());
                    return Behaviors.same();
                })
                .build();
    }

    /**
     * Handler for ReserveStockTell command.
     * Processes reservation and tells the result back.
     */
    private Behavior<Command> onReserveStockTell(ReserveStockTell cmd) {
        Product product = this.product;
        boolean success = false;
        String errorMessage = "";

        if (product == null) {
            errorMessage = "Product not found";
        } else if (product.stock_quantity >= cmd.quantity) {
            // If we have enough stock, decrease it and flag success
            product.stock_quantity -= cmd.quantity;
            success = true;
        } else {
            errorMessage = "Insufficient stock";
        }

        // Tell the response back to the calling actor
        cmd.replyTo.tell(new OnePlaceOrder.StockReservationResponse(
                cmd.productId, success, errorMessage));

        return this;
    }

    /**
     * Handler for GetProductDetailsTell command.
     * Sends product details back via tell pattern.
     */
    private Behavior<Command> onGetProductDetailsTell(GetProductDetailsTell cmd) {
        // Tell the product details back to the calling actor
        cmd.replyTo.tell(new OnePlaceOrder.ProductDetailsResponse(
                cmd.productId, this.product));

        return this;
    }

    /**
     * Handler for AddToStock command.
     * Increases the product's stock quantity and confirms success.
     */
    private Behavior<Command> onAddToStock(AddToStock msg) {
        this.product.stock_quantity += msg.add_amount;
        msg.replyTo.tell(new ProdResponses.AddStatus(true));
        return this;
    }

    /**
     * Handler for AddToStockForDeleteOrder command.
     * Increases stock when an order is deleted and notifies the DeleteOrder actor.
     */
    private Behavior<Command> onAddToStockForDeleteOrder(AddToStockForDeleteOrder msg) {
        this.product.stock_quantity += msg.add_amount;
        msg.replyTo.tell(new OneDeleteOrder.Command() {});  // Send empty command as acknowledgment
        return this;
    }

    /**
     * Handler for SubtractFromStock command.
     * Decreases stock if sufficient quantity is available.
     * Returns success or failure status to the caller.
     */
    private Behavior<Command> onSubtractFromStock(SubtractFromStock msg) {
        if (msg.subtract_amount <= this.product.stock_quantity) {
            // If we have enough stock, subtract it
            this.product.stock_quantity -= msg.subtract_amount;
            System.out.println("Subsctract Amount: " + msg.subtract_amount + " Current Amount: " + product.stock_quantity + 1);
            msg.replyTo.tell(new ProdResponses.SubtractStatus(true));
        } else {
            // Not enough stock
            msg.replyTo.tell(new ProdResponses.SubtractStatus(false));
        }
        return this;
    }

    /**
     * Handler for GetProductDetails command.
     * Returns the complete product information to the requester.
     */
    private Behavior<Command> onGetProductDetails(GetProductDetails msg) {
        ProductFound productFound = new ProductFound(product);
        msg.replyTo.tell(productFound);
        return this;
    }
}