package com.example.Responses;

import com.example.ImportantActors.Oneproduct;
import com.example.Responses.ProductResponse;
import com.example.SerializableTraitClass;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
// product found response

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public class ProductFound extends SerializableTraitClass implements ProductResponse {
    public final Oneproduct.Product product;

    @JsonCreator
    public ProductFound(@JsonProperty("product") Oneproduct.Product product) {
        this.product = product;
    }
}