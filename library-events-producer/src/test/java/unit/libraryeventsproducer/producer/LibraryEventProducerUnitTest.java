package libraryeventsproducer.producer;

import com.apachekafka.libraryeventsproducer.domain.Book;
import com.apachekafka.libraryeventsproducer.domain.LibraryEvent;
import com.apachekafka.libraryeventsproducer.producer.LibraryEventProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerUnitTest {

    @Mock
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Spy
    ObjectMapper mapper = new ObjectMapper();

    @InjectMocks
    LibraryEventProducer libraryEventProducer;

    @Test
    void sendLibraryEventAsynchronous_failure() throws JsonProcessingException, ExecutionException, InterruptedException {

        // GIVEN
        Book book = Book.builder()
                .bookId(123)
                .author("Ioannis Koinaris")
                .title("Kafka using SpringBoot").build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book).build();
        SettableListenableFuture future = new SettableListenableFuture();
        future.setException(new RuntimeException("Exception calling Kafka"));
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        // WHEN
        assertThrows(Exception.class, () -> libraryEventProducer.sendLibraryEventAsynchronous(libraryEvent).get());
    }

    @Test
    void sendLibraryEventAsynchronous_success() throws JsonProcessingException, ExecutionException, InterruptedException {

        // GIVEN
        Book book = Book.builder()
                .bookId(123)
                .author("Ioannis Koinaris")
                .title("Kafka using SpringBoot").build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book).build();
        String record = mapper.writeValueAsString(libraryEvent);
        SettableListenableFuture future = new SettableListenableFuture();
        ProducerRecord<Integer, String> producerRecord = new ProducerRecord("library-events", libraryEvent.getLibraryEventId(), record);

        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("library-events", 1),
                1, 1, 342, System.currentTimeMillis(), 1, 2);
        SendResult<Integer, String> result = new SendResult<Integer, String>(producerRecord, metadata);
        future.set(result);
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        // WHEN
        ListenableFuture<SendResult<Integer, String>> listenableFuture = libraryEventProducer.sendLibraryEventAsynchronous(libraryEvent);

        // THEN
        int partition = listenableFuture.get().getRecordMetadata().partition();
        assertEquals(partition, 1);
    }
}
