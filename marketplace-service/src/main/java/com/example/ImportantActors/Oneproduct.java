package com.example.ImportantActors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.serialization.jackson.JsonSerializable;
import com.example.Responses.ProductFound;
import com.example.Responses.ProductResponse;
import com.example.SerializableTraitClass;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Oneproduct actor represents a single product in an inventory system.
 * It handles operations like checking product details, managing stock levels,
 * and reserving inventory for orders.
 *
 * This actor is designed to work within an Akka Cluster Sharding context,
 * where each product instance is a separate actor identified by its product ID.
 */
public class Oneproduct extends AbstractBehavior<Oneproduct.Command> {

    // Logger for the actor
    private static final Logger logger = LoggerFactory.getLogger(Oneproduct.class);

    public static final EntityTypeKey<Oneproduct.Command> ENTITY_KEY =
            EntityTypeKey.create(Oneproduct.Command.class, "OneProduct");

    /**
     * Internal data structure that holds product information.
     * Note that stock_quantity is mutable while other properties are immutable.
     */
    public static class Product extends SerializableTraitClass implements JsonSerializable {
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

        @Override
        public String toString() {
            return String.format("Product(id=%d, name=%s, price=%d, stock=%d)",
                    id, name, price, stock_quantity);
        }
    }

    /**
     * Base interface for all commands that this actor can handle.
     * All command types must implement this marker interface.
     */
    public interface Command extends JsonSerializable { }

    /**
     * Confirmation message sent after restocking for order cancellation
     */
    public static class RestockConfirmation extends  SerializableTraitClass implements JsonSerializable {
        @JsonProperty("productId")
        public final int productId;
        @JsonProperty("quantity")
        public final int quantity;

        @JsonCreator
        public RestockConfirmation(
                @JsonProperty("productId") int productId,
                @JsonProperty("quantity") int quantity) {
            this.productId = productId;
            this.quantity = quantity;
        }
    }

    /**
     * Command to reserve a specific quantity of stock for an order being processed.
     * This is typically used in the first step of order processing before confirming payment.
     */
    public static class ReserveStock extends SerializableTraitClass implements Command {
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
     * Tell-based command to reserve stock
     */
    public static class ReserveStockTell extends  SerializableTraitClass implements Command {
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
     * Tell-based command to get product details
     */
    public static class GetProductDetailsTell extends SerializableTraitClass implements Command {
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
    public static class ReleaseReservation extends SerializableTraitClass implements Command {
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
    public static class AddToStock extends SerializableTraitClass implements Command {
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
     * Updated command to add stock when handling deleted orders.
     * Now includes replyTo that expects a RestockConfirmation response.
     */
    public static class AddToStockForDeleteOrder extends SerializableTraitClass implements Command {
        @JsonProperty("quantity")
        public final int quantity;                     // Amount to add back to stock
        @JsonProperty("replyTo")
        public final ActorRef<RestockConfirmation> replyTo;  // Actor to notify with confirmation

        @JsonCreator
        public AddToStockForDeleteOrder(
                @JsonProperty("quantity") int quantity,
                @JsonProperty("replyTo") ActorRef<RestockConfirmation> replyTo) {
            this.quantity = quantity;
            this.replyTo = replyTo;
        }
    }

    /**
     * Command to reduce the product's stock level.
     * Used when finalizing orders or removing damaged inventory.
     */
    public static class SubtractFromStock extends SerializableTraitClass implements Command {
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
    public static class GetProductDetails extends SerializableTraitClass implements Command {
        @JsonProperty("replyTo")
        public final ActorRef<ProductResponse> replyTo;

        @JsonCreator
        public GetProductDetails(@JsonProperty("replyTo") ActorRef<ProductResponse> replyTo) {
            this.replyTo = replyTo;
        }
    }

    public static final class Ping extends SerializableTraitClass implements Command {
        @JsonProperty("replyTo")
        public final ActorRef<Ack> replyTo;

        @JsonCreator
        public Ping(@JsonProperty("replyTo") ActorRef<Ack> replyTo) {
            this.replyTo = replyTo;
        }
    }

    public static class setProduct extends SerializableTraitClass implements Command {
        @JsonProperty("prod")
        public final Product prod;

        @JsonCreator
        public setProduct(@JsonProperty("prod") Product prod) {
            this.prod = prod;
        }
    }

    public static final class Ack {}

    // Internal state - the product this actor instance represents
    private Product product;

    /**
     * Private constructor used with the factory method.
     * Initializes the actor with a Product instance.
     */
    private Oneproduct(ActorContext<Command> context) {
        super(context);
        logger.info("Oneproduct actor created for product: {}", product);
    }

    /**
     * Factory method to create a new Oneproduct actor behavior.
     * This is the recommended way to instantiate Akka actors.
     */
    public static Behavior<Command> create() {
        return Behaviors.setup(context -> new Oneproduct(context));
    }

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
                .onMessage(ReserveStock.class, this::onReserveStock)
                .onMessage(ReserveStockTell.class, this::onReserveStockTell)
                .onMessage(GetProductDetailsTell.class, this::onGetProductDetailsTell)
                .onMessage(ReleaseReservation.class, this::onReleaseReservation)
                .onMessage(Ping.class, this::onPing)
                .onMessage(setProduct.class, msg -> {
                    this.product = msg.prod;
                    return this;
                })
                .build();
    }

    /**
     * Handler for Ping command.
     */
    private Behavior<Command> onPing(Ping msg) {
        msg.replyTo.tell(new Ack());
        return this;
    }

    /**
     * Handler for ReserveStock command.
     */
    private Behavior<Command> onReserveStock(ReserveStock cmd) {
        Product product = this.product;
        if (product == null) {
            // Fail if product doesn't exist
            logger.warn("Reservation failed: Product not found");
            cmd.replyTo.tell(new ProdResponses.ReservationFailure("Product not found"));
            return this;
        }

        if (product.stock_quantity >= cmd.quantity) {
            // If we have enough stock, decrease it and notify success
            product.stock_quantity -= cmd.quantity;
            logger.info("Reserved {} units of product {}, remaining stock: {}",
                    cmd.quantity, product.id, product.stock_quantity);
            cmd.replyTo.tell(new ProdResponses.ReservationSuccess(product.stock_quantity));
            return this;
        } else {
            // Not enough stock available
            logger.warn("Reservation failed: Insufficient stock (requested: {}, available: {})",
                    cmd.quantity, product.stock_quantity);
            cmd.replyTo.tell(new ProdResponses.ReservationFailure("Insufficient stock"));
            return this;
        }
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
            logger.warn("ReserveStockTell failed: Product not found");
            errorMessage = "Product not found";
        } else if (product.stock_quantity >= cmd.quantity) {
            // If we have enough stock, decrease it and flag success
            product.stock_quantity -= cmd.quantity;
            logger.info("ReserveStockTell: Reserved {} units of product {}, remaining stock: {}",
                    cmd.quantity, product.id, product.stock_quantity);
            success = true;
        } else {
            logger.warn("ReserveStockTell failed: Insufficient stock (requested: {}, available: {})",
                    cmd.quantity, product.stock_quantity);
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
        logger.info("Sending product details for product ID: {}", cmd.productId);

        // Tell the product details back to the calling actor
        cmd.replyTo.tell(new OnePlaceOrder.ProductDetailsResponse(
                cmd.productId, this.product));

        return this;
    }

    /**
     * Handler for ReleaseReservation command.
     */
    private Behavior<Command> onReleaseReservation(ReleaseReservation cmd) {
        Product product = this.product;
        if (product != null) {
            // Return the reserved quantity back to available stock
            product.stock_quantity += cmd.quantity;
            logger.info("Released {} units back to stock for product {}, new stock: {}",
                    cmd.quantity, product.id, product.stock_quantity);
        } else {
            logger.warn("ReleaseReservation: Product not found");
        }
        return this;
    }

    /**
     * Handler for AddToStock command.
     * Increases the product's stock quantity and confirms success.
     */
    private Behavior<Command> onAddToStock(AddToStock msg) {
        if (this.product == null) {
            logger.warn("AddToStock failed: Product not found");
            msg.replyTo.tell(new ProdResponses.AddStatus(false));
            return this;
        }

        this.product.stock_quantity += msg.add_amount;
        logger.info("Added {} units to stock for product {}, new stock: {}",
                msg.add_amount, product.id, product.stock_quantity);
        msg.replyTo.tell(new ProdResponses.AddStatus(true));
        return this;
    }

    /**
     * Updated handler for AddToStockForDeleteOrder command.
     * Now sends back a RestockConfirmation message as confirmation.
     */
    private Behavior<Command> onAddToStockForDeleteOrder(AddToStockForDeleteOrder msg) {
        if (this.product == null) {
            logger.warn("AddToStockForDeleteOrder failed: Product not found");
            // Even if product not found, send confirmation to avoid blocking the workflow
            msg.replyTo.tell(new RestockConfirmation(-1, msg.quantity));
            return this;
        }

        this.product.stock_quantity += msg.quantity;
        logger.info("Restocked {} units for product {} due to order cancellation, new stock: {}",
                msg.quantity, product.id, product.stock_quantity);

        // Send confirmation message back to the sender
        msg.replyTo.tell(new RestockConfirmation(product.id, msg.quantity));
        return this;
    }

    /**
     * Handler for SubtractFromStock command.
     * Decreases stock if sufficient quantity is available.
     * Returns success or failure status to the caller.
     */
    private Behavior<Command> onSubtractFromStock(SubtractFromStock msg) {
        if (this.product == null) {
            logger.warn("SubtractFromStock failed: Product not found");
            msg.replyTo.tell(new ProdResponses.SubtractStatus(false));
            return this;
        }

        if (msg.subtract_amount <= this.product.stock_quantity) {
            // If we have enough stock, subtract it
            this.product.stock_quantity -= msg.subtract_amount;
            logger.info("Subtracted {} units from product {}, remaining stock: {}",
                    msg.subtract_amount, product.id, product.stock_quantity);
            msg.replyTo.tell(new ProdResponses.SubtractStatus(true));
        } else {
            // Not enough stock
            logger.warn("SubtractFromStock failed: Insufficient stock (requested: {}, available: {})",
                    msg.subtract_amount, product.stock_quantity);
            msg.replyTo.tell(new ProdResponses.SubtractStatus(false));
        }
        return this;
    }

    /**
     * Handler for GetProductDetails command.
     * Returns the complete product information to the requester.
     */
    private Behavior<Command> onGetProductDetails(GetProductDetails msg) {
        if (this.product == null) {
            logger.warn("GetProductDetails failed: Product not found");
            // Here you would typically send a "not found" response
            return this;
        }

        logger.info("Sending full product details for product ID: {}", product.id);
        ProductFound productFound = new ProductFound(product);
        msg.replyTo.tell(productFound);
        return this;
    }
}