package com.example.ImportantActors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum OrderStatus {

    PLACED,
    CANCELLED,
    DELIVERED
}
