package com.example.Responses;

import com.example.ImportantActors.Oneproduct;
import com.example.Responses.ProductResponse;
// product found response
public class ProductFound implements ProductResponse {
    public final Oneproduct.Product product;

    public ProductFound(Oneproduct.Product product) {
        this.product = product;
    }
}
