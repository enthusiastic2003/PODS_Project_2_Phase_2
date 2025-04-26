package com.example.ImportantActors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.HttpMethods;
import akka.http.javadsl.model.HttpRequest;
import com.example.SerializableTraitClass;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;

// Main actor class for managing user discounts
public class DiscountManager extends AbstractBehavior<DiscountManager.Command> {

    // Interface for response messages
    public interface Response {}

    // Interface for commands this actor can handle
    public interface Command {}

    // HTTP client for external service calls
    private final Http http;

    // Local store mapping user IDs to their discount status
    private HashMap<Integer, Boolean> userToDiscount;

    // Key for cluster sharding identification
    public static final EntityTypeKey<Command> ENTITY_KEY =
            EntityTypeKey.create(Command.class, "DiscountManager");

    // Response message for discount status queries
    public static class GetDiscountResponse extends SerializableTraitClass implements Response {
        boolean discount_availed;

        @JsonCreator  // Constructor to deserialize JSON into this class
        public GetDiscountResponse(@JsonProperty("discount_availed") boolean discount_availed) {
            this.discount_availed = discount_availed;
        }
    }

    // Command to check a user's discount status (original ask pattern)
    public static class GetDiscountStatus extends SerializableTraitClass implements Command {
        Integer user_id;
        ActorRef<GetDiscountResponse> replyTo;
        boolean presumed_discount_status;  // Default status if user not found

        @JsonCreator  // Constructor to deserialize JSON into this class
        public GetDiscountStatus(
                @JsonProperty("user_id") Integer user_id,
                @JsonProperty("presumed_discount_status") boolean presumed_discount_status,
                @JsonProperty("replyTo") ActorRef<GetDiscountResponse> replyTo) {
            this.user_id = user_id;
            this.replyTo = replyTo;
            this.presumed_discount_status = presumed_discount_status;
        }
    }

    // NEW: Command to check a user's discount status (tell pattern)
    public static class GetDiscountStatusTell extends SerializableTraitClass implements Command {
        Integer user_id;
        boolean presumed_discount_status;
        ActorRef<OnePlaceOrder.Command> replyTo;

        @JsonCreator
        public GetDiscountStatusTell(
                @JsonProperty("user_id") Integer user_id,
                @JsonProperty("presumed_discount_status") boolean presumed_discount_status,
                @JsonProperty("replyTo") ActorRef<OnePlaceOrder.Command> replyTo) {
            this.user_id = user_id;
            this.presumed_discount_status = presumed_discount_status;
            this.replyTo = replyTo;
        }
    }

    // Command to apply a discount for a user (original ask pattern)
    public static class ApplyDiscount extends SerializableTraitClass implements Command {
        Integer user_id;
        ActorRef<GetDiscountResponse> replyTo;

        @JsonCreator  // Constructor to deserialize JSON into this class
        public ApplyDiscount(
                @JsonProperty("user_id") Integer user_id,
                @JsonProperty("replyTo") ActorRef<GetDiscountResponse> replyTo) {
            this.user_id = user_id;
            this.replyTo = replyTo;
        }
    }

    // NEW: Command to apply a discount for a user (tell pattern)
    public static class ApplyDiscountTell extends SerializableTraitClass implements Command {
        Integer user_id;
        ActorRef<OnePlaceOrder.Command> replyTo;

        @JsonCreator
        public ApplyDiscountTell(
                @JsonProperty("user_id") Integer user_id,
                @JsonProperty("replyTo") ActorRef<OnePlaceOrder.Command> replyTo) {
            this.user_id = user_id;
            this.replyTo = replyTo;
        }
    }

    // Command to release a discount (update external service)
    public static class ReleaseDiscount extends SerializableTraitClass implements Command {
        Integer user_id;

        @JsonCreator  // Constructor to deserialize JSON into this class
        public ReleaseDiscount(@JsonProperty("user_id") Integer user_id) {
            this.user_id = user_id;
        }
    }

    // Command to revert a discount lock
    public static class RevertDiscountLock extends SerializableTraitClass implements Command {
        Integer user_id;

        @JsonCreator  // Constructor to deserialize JSON into this class
        public RevertDiscountLock(@JsonProperty("user_id") Integer user_id) {
            this.user_id = user_id;
        }
    }

    // Constructor initializing the actor
    private DiscountManager(ActorContext<Command> context) {
        super(context);
        this.http = Http.get(context.getSystem());  // Get HTTP client
        this.userToDiscount = new HashMap<>();  // Initialize discount store
    }

    // Factory method for creating the actor
    public static Behavior<Command> create() {
        return Behaviors.setup(context -> new DiscountManager(context));
    }

    // Define message handlers
    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(ApplyDiscount.class, this::onApplyDiscount)
                .onMessage(ApplyDiscountTell.class, this::onApplyDiscountTell)
                .onMessage(GetDiscountStatus.class, this::onGetDiscountStatus)
                .onMessage(GetDiscountStatusTell.class, this::onGetDiscountStatusTell)
                .onMessage(ReleaseDiscount.class, this::onReleaseDiscount)
                .onMessage(RevertDiscountLock.class, this::onRevertDiscountLock)
                .build();
    }

    // Handler for reverting discount locks
    public Behavior<Command> onRevertDiscountLock(RevertDiscountLock revertDiscountLock) {
        if (userToDiscount.getOrDefault(revertDiscountLock.user_id, false)) {
            userToDiscount.put(revertDiscountLock.user_id, false);
        }
        return this;
    }

    // Handler for releasing discounts (external service update)
    public Behavior<Command> onReleaseDiscount(ReleaseDiscount command) {
        String userServiceUrl = getContext().getSystem().settings().config()
                .getString("my-app.routes.user-service") + command.user_id;

        HttpRequest discountUpdateRequest = HttpRequest.create()
                .withMethod(HttpMethods.PUT)
                .withUri(userServiceUrl);

        http.singleRequest(discountUpdateRequest).thenAccept(response -> {
            if (response.status().isSuccess()) {
                getContext().getLog().info("Discount status updated successfully for user {}",
                        command.user_id);
            } else {
                getContext().getLog().error("Failed to update discount status for user {}: {}",
                        command.user_id, response.status());
            }
        });

        return this;
    }

    // Original handler for discount status queries (ask pattern)
    public Behavior<Command> onGetDiscountStatus(GetDiscountStatus getDiscountStatus) {
        if (userToDiscount.containsKey(getDiscountStatus.user_id)) {
            boolean status = userToDiscount.get(getDiscountStatus.user_id);
            getDiscountStatus.replyTo.tell(new GetDiscountResponse(status));
        } else {
            userToDiscount.put(getDiscountStatus.user_id, getDiscountStatus.presumed_discount_status);
            getDiscountStatus.replyTo.tell(
                    new GetDiscountResponse(getDiscountStatus.presumed_discount_status));
        }
        return this;
    }

    // NEW: Handler for discount status queries (tell pattern)
    public Behavior<Command> onGetDiscountStatusTell(GetDiscountStatusTell command) {
        boolean status;
        if (userToDiscount.containsKey(command.user_id)) {
            status = userToDiscount.get(command.user_id);
        } else {
            userToDiscount.put(command.user_id, command.presumed_discount_status);
            status = command.presumed_discount_status;
        }

        // Tell the OnePlaceOrder actor about the discount status
        command.replyTo.tell(new OnePlaceOrder.DiscountStatusResponse(
                command.user_id, status));

        return this;
    }

    // Original handler for applying discounts (ask pattern)
    public Behavior<Command> onApplyDiscount(ApplyDiscount msg) {
        Integer user_id = msg.user_id;
        boolean wasDiscountAvailed = userToDiscount.getOrDefault(user_id, false);

        if (!wasDiscountAvailed) {
            userToDiscount.put(user_id, true);
            msg.replyTo.tell(new GetDiscountResponse(false));
        } else {
            msg.replyTo.tell(new GetDiscountResponse(true));
        }
        return this;
    }

    // NEW: Handler for applying discounts (tell pattern)
    public Behavior<Command> onApplyDiscountTell(ApplyDiscountTell command) {
        Integer user_id = command.user_id;
        boolean wasDiscountAvailed = userToDiscount.getOrDefault(user_id, false);

        if (!wasDiscountAvailed) {
            userToDiscount.put(user_id, true);
        }

        // Tell the OnePlaceOrder actor about the discount application
        command.replyTo.tell(new OnePlaceOrder.ApplyDiscountResponse(user_id, !wasDiscountAvailed));


        return this;
    }
}