package com.apachekafka.libraryeventsproducer.controller;

import com.apachekafka.libraryeventsproducer.domain.Book;
import com.apachekafka.libraryeventsproducer.domain.LibraryEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class LibraryEventsControllerIntegrationTest {

    @Autowired
    TestRestTemplate testRestTemplate;

    @Test
    void postLibraryEvent() {

        // given
        Book book = Book.builder()
                .id(123)
                .author("Ioannis Koinaris")
                .title("Kafka using SpringBoot").build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .id(null)
                .book(book).build();

        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());
        HttpEntity<LibraryEvent> entity = new HttpEntity<>(libraryEvent);

        // when
        ResponseEntity<LibraryEvent> responseEntity =
                testRestTemplate.exchange("/v1/libraryevent", HttpMethod.POST, entity, LibraryEvent.class);

        // then
        assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());
    }
}
