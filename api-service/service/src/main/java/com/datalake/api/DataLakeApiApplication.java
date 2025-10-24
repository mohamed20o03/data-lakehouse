package com.datalake.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

@SpringBootApplication
public class DataLakeApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataLakeApiApplication.class, args);
    }

    /* 
        This method defines a Spring Bean of type ObjectMapper.
        Spring will create and manage this single ObjectMapper instance for the whole application (Singleton scope).
        The JavaTimeModule is registered to handle Java 8 date and time types (e.g., LocalDate, LocalDateTime)
        correctly during JSON serialization and deserialization.
    */
    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }
}