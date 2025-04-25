package com.example.Requests;

import com.example.SerializableTraitClass;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
// used to make json for product request
public class OrderItemRequests extends SerializableTraitClass {
    public final Integer product_id;
    public final Integer quantity;

    @JsonCreator
    public OrderItemRequests(
            @JsonProperty("product_id") Integer product_id,
            @JsonProperty("quantity") Integer quantity) {

        this.product_id = product_id;
        this.quantity = quantity;
    }
}