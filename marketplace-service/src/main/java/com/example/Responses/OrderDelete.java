package com.example.Responses;

import com.example.SerializableTraitClass;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

// different responses for order delete
public interface OrderDelete {
    class Success extends SerializableTraitClass implements Response {
        public final String message;

        @JsonCreator
        public Success(@JsonProperty("message") String message) {
            this.message = message;
        }
    }

    class Failure extends SerializableTraitClass implements Response {
        public final String message;

        @JsonCreator
        public Failure(@JsonProperty("message") String message) {
            this.message = message;
        }
    }

    interface Response {}
}