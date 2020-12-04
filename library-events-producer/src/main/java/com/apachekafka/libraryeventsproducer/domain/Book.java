package com.apachekafka.libraryeventsproducer.domain;

import com.sun.istack.internal.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class Book {

    @NotNull
    private Integer id;

    @NotNull
    private String title;

    @NotNull
    private String author;
}
