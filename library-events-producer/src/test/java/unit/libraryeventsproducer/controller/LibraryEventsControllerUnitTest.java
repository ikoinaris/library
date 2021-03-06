package libraryeventsproducer.controller;

import com.apachekafka.libraryeventsproducer.controller.LibraryEventsController;
import com.apachekafka.libraryeventsproducer.domain.Book;
import com.apachekafka.libraryeventsproducer.domain.LibraryEvent;
import com.apachekafka.libraryeventsproducer.producer.LibraryEventProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.protocol.types.Field;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;


import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc
public class LibraryEventsControllerUnitTest {
    
    @Autowired
    MockMvc mockMvc;

    ObjectMapper mapper = new ObjectMapper();

    @MockBean
    LibraryEventProducer libraryEventProducer;

    @Test
    void postLibraryEvent() throws Exception {

        // given
        Book book = Book.builder()
                .id(123)
                .author("Ioannis Koinaris")
                .title("Kafka using SpringBoot").build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .id(null)
                .book(book).build();

        String json = mapper.writeValueAsString(libraryEvent);
        when(libraryEventProducer.sendLibraryEventAsynchronous(isA(LibraryEvent.class))).thenReturn(null);

        // when
        mockMvc.perform(post("/v1/libraryevent")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON)).andExpect(status().isCreated());
    }

    @Test
    void postLibraryEvent_4xx() throws Exception {

        // given
        Book book = Book.builder()
                .id(null)
                .author(null)
                .title("Kafka using SpringBoot").build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .id(null)
                .book(book).build();

        String json = mapper.writeValueAsString(libraryEvent);
        when(libraryEventProducer.sendLibraryEventAsynchronous(isA(LibraryEvent.class))).thenReturn(null);

        // when
        String expectedErrorMessage = "";
        mockMvc.perform(post("/v1/libraryevent")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON)).andExpect(status().is4xxClientError()).andExpect(content().string(expectedErrorMessage));
    }
}
