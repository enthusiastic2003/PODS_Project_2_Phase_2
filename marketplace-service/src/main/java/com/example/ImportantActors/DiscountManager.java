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

    // Command to check a user's discount status
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

    // Command to apply a discount for a user
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
                .onMessage(GetDiscountStatus.class, this::onGetDiscountStatus)
                .onMessage(ReleaseDiscount.class, this::onReleaseDiscount)
                .onMessage(RevertDiscountLock.class, this::onRevertDiscountLock)
                .build();
    }

    // Handler for reverting discount locks
    public Behavior<Command> onRevertDiscountLock(RevertDiscountLock revertDiscountLock) {
        if (userToDiscount.get(revertDiscountLock.user_id) == true) {
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

    // Handler for discount status queries
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

    // Handler for applying discounts
    public Behavior<Command> onApplyDiscount(ApplyDiscount msg) {
        Integer user_id = msg.user_id;
        if (userToDiscount.getOrDefault(user_id, false) == false) {
            userToDiscount.put(user_id, true);
            msg.replyTo.tell(new GetDiscountResponse(false));
        } else {
            msg.replyTo.tell(new GetDiscountResponse(true));
        }
        return this;
    }
}
