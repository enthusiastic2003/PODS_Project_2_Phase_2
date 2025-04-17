package com.example.Responses;
// different responses for order delete
public interface OrderDelete {
    class Success implements Response {
        public final String message;

        public Success(String message) {
            this.message = message;
        }
    }

    class Failure implements Response {
        public final String message;

        public Failure(String message) {
            this.message = message;
        }
    }

    interface Response {}
}