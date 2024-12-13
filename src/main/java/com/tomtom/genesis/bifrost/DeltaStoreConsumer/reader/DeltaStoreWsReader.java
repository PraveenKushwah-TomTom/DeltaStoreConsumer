package com.tomtom.genesis.bifrost.DeltaStoreConsumer.reader;


import com.tomtom.genesis.bifrost.deltastore.model.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Optional;

@Component
public class DeltaStoreWsReader {

    public static final String ID_PLACEHOLDER_STR = "{id}";
    public static final String CREATED_UNTIL_PARAM = "created_until";
    private static final String CREATED_SINCE_PARAM = "created_since";
    private static final String ZONE_PARAMS = "zone";
    private static final String CHANGE_IDS_PARAMS = "change_ids";
    private static final String TRACES = "traces";
    public static final String CHANGES = "changes";
    public static final String ZONES = "zones";
    public static final String LICENSE = "license";
    public static final String HTTPS_PROTO = "https";
    public static final String CREATED_AT = "created_at";
    public static final String FEATURE = "feature.type";
    private final String host;
    private final WebClient webClient;

    public DeltaStoreWsReader(@Value("${bifrost.delta.store.ws.host}")String host, WebClient webClient) {
        this.host = host;
        this.webClient = webClient;
    }

    public Flux<MapChange> getChangesFlux(final String zone, final String createdSince,
                                          final String createdUntil, final String license, final String feature) {
        return webClient.get().uri(uriBuilder -> uriBuilder.scheme(HTTPS_PROTO)
            .host(host)
            .port(443)
            .path(CHANGES)
            .queryParam(ZONE_PARAMS, zone)
            .queryParam(CREATED_SINCE_PARAM, createdSince)
            .queryParam(CREATED_UNTIL_PARAM, createdUntil)
            .queryParam(LICENSE, license)
            .queryParam(FEATURE, feature)
            .build())
            .retrieve()
            .bodyToMono(ChangesResponse.class)
            .expand(this::getNext)
            .flatMap(changeResponse -> Flux.fromIterable(changeResponse.getChanges()));
    }

    private Mono<ChangesResponse> getNext(final ChangesResponse changesResponse) {
        //log.info("Fetching Next: " + changesResponse.getNext());
        return Optional.ofNullable(changesResponse.getNext())
            .map(next -> webClient.get().uri(next)
                .retrieve()
                .bodyToMono(ChangesResponse.class))
            .orElse(Mono.empty());
    }

    public Flux<TracesResponse> getTracesFlux(final String license, final String changeIds) {
        return webClient.get().uri(uriBuilder -> uriBuilder.scheme(HTTPS_PROTO)
                .host(host)
                .port(443)
                .path(TRACES)
                .queryParam(LICENSE, license)
                .queryParam(CHANGE_IDS_PARAMS, changeIds)
                .build())
            .retrieve()
            .bodyToMono(TracesResponse.class)
            .expand(this::getNextTrace);
    }

    public Flux<TracesResponse> getTracesFlux(final String license, final String zoneId, final String createdAt) {
        return webClient.get().uri(uriBuilder -> uriBuilder.scheme(HTTPS_PROTO)
                .host(host)
                .port(443)
                .path(TRACES)
                .queryParam(LICENSE, license)
                .queryParam(ZONE_PARAMS, zoneId)
                .queryParam(CREATED_AT, createdAt)
                .build())
            .retrieve()
            .bodyToMono(TracesResponse.class)
            .expand(this::getNextTrace);
    }

    private Mono<TracesResponse> getNextTrace(final TracesResponse tracesResponse) {
       // log.info("Fetching Next: " + tracesResponse.getNext());
        return Optional.ofNullable(tracesResponse.getNext())
            .map(next -> webClient.get().uri(next)
                .retrieve()
                .bodyToMono(TracesResponse.class))
            .orElse(Mono.empty());
    }

    public Mono<ZonesResponse> getZones(final String license) {
        return webClient.get().uri(uriBuilder -> uriBuilder.scheme(HTTPS_PROTO)
            .host(host)
            .port(443)
            .path(ZONES)
            .queryParam(LICENSE, license)
            .build())
            .retrieve()
            .bodyToMono(ZonesResponse.class);

    }

    public Mono<MapZone> getZone(final String zoneId, final String license) {
        return webClient.get().uri(uriBuilder -> uriBuilder.scheme(HTTPS_PROTO)
            .host(host)
            .port(443)
            .queryParam(LICENSE, license)
            .pathSegment(ZONES, ID_PLACEHOLDER_STR)
            .build(zoneId))
            .retrieve()
            .bodyToMono(MapZone.class);
    }
}