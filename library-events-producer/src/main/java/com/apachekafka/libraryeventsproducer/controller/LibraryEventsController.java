package com.apachekafka.libraryeventsproducer.controller;

import com.apachekafka.libraryeventsproducer.domain.LibraryEvent;
import com.apachekafka.libraryeventsproducer.domain.LibraryEventType;
import com.apachekafka.libraryeventsproducer.producer.LibraryEventProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
@Slf4j
public class LibraryEventsController {

    @Autowired
    LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent)
            throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        libraryEventProducer.sendLibraryEventAsynchronous(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/v1/libraryevent")
    public ResponseEntity<?> putLibraryEvent(@RequestBody LibraryEvent libraryEvent)
            throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

        if(libraryEvent.getId() == null){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the libraryId");
        }
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEventProducer.sendLibraryEventAsynchronous(libraryEvent);
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }
}
