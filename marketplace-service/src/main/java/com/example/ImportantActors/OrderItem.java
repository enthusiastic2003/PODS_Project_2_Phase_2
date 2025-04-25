package com.example.ImportantActors;

import com.example.SerializableTraitClass;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class OrderItem extends SerializableTraitClass {

    public Integer id;
    public Integer product_id;
    public Integer quantity;

    // JsonCreator for deserialization from JSON
    @JsonCreator
    public OrderItem(
            @JsonProperty("id") Integer id,
            @JsonProperty("product_id") Integer product_id,
            @JsonProperty("quantity") Integer quantity
    ) {
        this.id = id;
        this.product_id = product_id;
        this.quantity = quantity;
    }
}
