package com.example.Responses;

import com.example.SerializableTraitClass;

// product not found response
public class ProductNotFound extends SerializableTraitClass implements ProductResponse {
    public final String message = "Product not found";
}
