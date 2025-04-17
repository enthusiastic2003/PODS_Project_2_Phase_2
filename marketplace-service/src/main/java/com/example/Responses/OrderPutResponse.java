package com.example.Responses;

import akka.http.javadsl.model.StatusCode;
// Order Put Response for updating order
public class OrderPutResponse {


        public String message;
        public StatusCode status;

        public OrderPutResponse(StatusCode status, String message) {
            this.message = message;
            this.status = status;
        }




}
