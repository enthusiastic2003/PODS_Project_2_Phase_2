package com.example.Responses;

import akka.http.javadsl.model.StatusCode;
import akka.http.javadsl.model.StatusCodes;
import com.example.SerializableTraitClass;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class OrderPutResponse extends SerializableTraitClass {

    @JsonProperty("message")
    public String message;

    @JsonProperty("status")
    public int status;  // Store StatusCode as String

    // Constructor for Jackson deserialization
    @JsonCreator
    public OrderPutResponse(
            @JsonProperty("status") int status,  // Deserialize status as String
            @JsonProperty("message") String message) {
        this.message = message;
        this.status = status;
    }

    // Convert StatusCode to String when creating the response
    public static OrderPutResponse fromStatusCode(StatusCode status, String message) {
        return new OrderPutResponse(Integer.parseInt(status.toString()), message);
    }

    // Convert the stored String back to StatusCode
    public StatusCode getStatusCode() {
        return StatusCodes.get(status);
    }
}
