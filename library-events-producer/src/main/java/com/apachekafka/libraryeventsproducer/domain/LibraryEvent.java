package com.apachekafka.libraryeventsproducer.domain;

import com.sun.istack.internal.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data // generate all getters and setters
@Builder
public class LibraryEvent {

    private Integer id;

    private LibraryEventType libraryEventType;

    @NotNull
    private Book book;
}
