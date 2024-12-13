package com.tomtom.genesis.bifrost.DeltaStoreConsumer;

import com.tomtom.genesis.bifrost.DeltaStoreConsumer.reader.DeltaStoreWsReader;
import com.tomtom.genesis.bifrost.deltastore.model.MapChange;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.util.List;

@Service
public class RunnerService {

    @Autowired
    private DeltaStoreWsReader deltaStoreWsReader;

    public void invokeDSWS(String zone, String createdSince, String createdUntil, String license, String feature) {
        Flux<MapChange> mapChanges = Flux.empty();
        final Flux<MapChange> zoneChangesFlux =deltaStoreWsReader.getChangesFlux(zone, createdSince, createdUntil, license, feature);
        mapChanges = mapChanges.concatWith(zoneChangesFlux);
        List<MapChange> changeList = mapChanges.collectList().block();
        System.out.println("Total Changes: "+ changeList.size());
        changeList.stream().map(MapChange::getId).forEach(System.out::println);

    }
}
