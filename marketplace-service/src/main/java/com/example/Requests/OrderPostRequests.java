package com.example.Requests;

import com.fasterxml.jackson.annotation.JsonCreator;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

// used to make json for order request
public class OrderPostRequests {
    public final Integer user_id;
    public final List<OrderItemRequests> items;

    @JsonCreator
    public OrderPostRequests(
            @JsonProperty("user_id") Integer user_id,
            @JsonProperty("items") List<OrderItemRequests> items) {

        this.user_id = user_id;
        this.items = items;
    }


}
