package com.tomtom.genesis.bifrost.DeltaStoreConsumer;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@RequiredArgsConstructor
@EnableAutoConfiguration
public class DeltaStoreConsumerApplication implements CommandLineRunner {
    public static void main(String[] args) {
        SpringApplication.run(DeltaStoreConsumerApplication.class, args);
    }

    @Autowired
    private RunnerService runnerService;

    @Override
    public void run(String... args) throws Exception {
        runnerService.invokeDSWS("BEL",
                "2024-08-22",
                "2024-08-22",
                "TTOM",
                "TTOM-POI::Airport");
    }
}
