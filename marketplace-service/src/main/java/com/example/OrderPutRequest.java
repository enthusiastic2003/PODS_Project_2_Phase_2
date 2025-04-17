package com.example;

import com.example.ImportantActors.OrderStatus;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class OrderPutRequest {
    public final Integer order_id;
    public final OrderStatus status;

    @JsonCreator
    public OrderPutRequest(
            @JsonProperty("order_id") Integer order_id,
            @JsonProperty("status") OrderStatus order_status) {

        this.order_id = order_id;
        this.status = order_status;
    }
}
