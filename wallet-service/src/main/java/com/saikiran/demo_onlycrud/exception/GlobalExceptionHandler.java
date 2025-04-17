// package com.saikiran.demo_onlycrud.exception;

// import org.springframework.http.HttpStatus;
// import org.springframework.http.ResponseEntity;
// import org.springframework.web.bind.annotation.ControllerAdvice;
// import org.springframework.web.bind.annotation.ExceptionHandler;
// import org.springframework.web.server.ResponseStatusException;

// @ControllerAdvice
// public class GlobalExceptionHandler {

//     @ExceptionHandler(ResponseStatusException.class)
//     public ResponseEntity<String> handleResponseStatusException(ResponseStatusException ex) {
//         return new ResponseEntity<>(ex.getReason(), ex.getStatusCode());
//     }

//     @ExceptionHandler(Exception.class)
//     public ResponseEntity<String> handleException(Exception ex) {
//         return new ResponseEntity<>("An unexpected error occurred: " + ex.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
//     }
// }