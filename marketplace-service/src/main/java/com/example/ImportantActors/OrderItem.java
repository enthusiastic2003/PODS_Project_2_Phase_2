package com.example.ImportantActors;

public class OrderItem {

    public Integer id;
    public Integer product_id;
    public Integer quantity;

    public OrderItem(Integer order_id, Integer product_id, Integer quantity) {
        this.id = order_id;
        this.product_id = product_id;
        this.quantity = quantity;

    }
}
