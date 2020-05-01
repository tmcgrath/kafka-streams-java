package com.supergloo;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

public class KafkaStreamsJoins {


    public static void main(String[] args) throws Exception {
    }

    static Topology kTableToKTableJoin(String inputTopic1,
                                       String inputTopic2,
                                       String storeName) {

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> userRegions = builder.table(inputTopic1);
        KTable<String, Long> regionMetrics = builder.table(inputTopic2, Consumed.with(Serdes.String(), Serdes.Long()));

        KTable<String, String> joined = userRegions.join(regionMetrics,
                (regionValue, metricValue) -> regionValue + "/" + metricValue.toString(),
                Materialized.as(storeName)
        );

        return builder.build();
    }


    static Topology kTableToKTableLeftJoin(String inputTopic1,
                                           String inputTopic2,
                                           String storeName) {

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> userRegions = builder.table(inputTopic1);
        KTable<String, Long> regionMetrics = builder.table(inputTopic2, Consumed.with(Serdes.String(), Serdes.Long()));

        userRegions.leftJoin(regionMetrics,
                (regionValue, metricValue) -> regionValue + "/" + metricValue,
                Materialized.as(storeName)
        );

        return builder.build();
    }

    static Topology kTableToKTableOuterJoin(String inputTopic1,
                                             String inputTopic2,
                                             String storeName) {

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> userRegions = builder.table(inputTopic1);
        KTable<String, Long> regionMetrics = builder.table(inputTopic2, Consumed.with(Serdes.String(), Serdes.Long()));

        userRegions.outerJoin(regionMetrics,
                (regionValue, metricValue) -> regionValue + "/" + metricValue,
                Materialized.as(storeName)
        );

        return builder.build();
    }

    static Topology kStreamToKTableJoin(String inputTopic1,
                                        String inputTopic2,
                                        String outputTopicName,
                                        String storeName) {

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> userRegions = builder.table(inputTopic1);
        KStream<String, Long> regionMetrics = builder.stream(inputTopic2, Consumed.with(Serdes.String(), Serdes.Long()));

        regionMetrics.join(userRegions,
                (regionValue, metricValue) -> regionValue + "/" + metricValue).to(outputTopicName);

        builder.table(outputTopicName, Materialized.as(storeName));

        return builder.build();
    }

    static Topology kStreamToKTableLeftJoin(String inputTopic1,
                                String inputTopic2,
                                String outputTopicName,
                                String storeName) {

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> userRegions = builder.table(inputTopic1);
        KStream<String, Long> regionMetrics = builder.stream(inputTopic2, Consumed.with(Serdes.String(), Serdes.Long()));

        regionMetrics.leftJoin(userRegions,
                (regionValue, metricValue) -> regionValue + "/" + metricValue).to(outputTopicName);

        builder.table(outputTopicName, Materialized.as(storeName));

        return builder.build();
    }

    static Topology kStreamToKTableOuterJoin(String inputTopic1,
                                 String inputTopic2,
                                 String outputTopicName,
                                 String storeName) {

        return new StreamsBuilder().build();
    }


    // -------- KStream to KStream joins ------------//

    static Topology kStreamToKStreamJoin(String inputTopic1,
                             String inputTopic2,
                             String outputTopicName,
                             String storeName) {

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> userRegions = builder.stream(inputTopic1, Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, Long> regionMetrics = builder.stream(inputTopic2, Consumed.with(Serdes.String(), Serdes.Long()));

        regionMetrics.join(userRegions,
                            (regionValue, metricValue) -> regionValue + "/" + metricValue,
                            JoinWindows.of(Duration.ofMinutes(5L)),
                             StreamJoined.with(Serdes.String(), Serdes.Long(), Serdes.String())
        ).to(outputTopicName);

        builder.table(outputTopicName, Materialized.as(storeName));

        return builder.build();
    }

    static Topology kStreamToKStreamLeftJoin(String inputTopic1,
                                             String inputTopic2,
                                             String outputTopicName,
                                             String storeName) {

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> userRegions = builder.stream(inputTopic1, Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, Long> regionMetrics = builder.stream(inputTopic2, Consumed.with(Serdes.String(), Serdes.Long()));

        regionMetrics.leftJoin(userRegions,
                (regionValue, metricValue) -> regionValue + "/" + metricValue,
                JoinWindows.of(Duration.ofMinutes(5L)),
                StreamJoined.with(Serdes.String(), Serdes.Long(), Serdes.String())
        ).to(outputTopicName);

        builder.table(outputTopicName, Materialized.as(storeName));

        return builder.build();
    }

    static Topology kStreamToKStreamOuterJoin(String inputTopic1,
                                              String inputTopic2,
                                              String outputTopicName,
                                              String storeName) {

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> userRegions = builder.stream(inputTopic1, Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, Long> regionMetrics = builder.stream(inputTopic2, Consumed.with(Serdes.String(), Serdes.Long()));

        regionMetrics.outerJoin(userRegions,
                (regionValue, metricValue) -> regionValue + "/" + metricValue,
                JoinWindows.of(Duration.ofMinutes(5L)),
                StreamJoined.with(Serdes.String(), Serdes.Long(), Serdes.String())
        ).to(outputTopicName);

        builder.table(outputTopicName, Materialized.as(storeName));

        return builder.build();
    }

    // -------- KStream to GlobalKTable joins ------------//

    static Topology kStreamToGlobalKTableJoin(String inputTopic1,
                                              String inputTopic2,
                                              String outputTopicName,
                                              String storeName) {

        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, String> userRegions = builder.globalTable(inputTopic1);
        KStream<String, Long> regionMetrics = builder.stream(inputTopic2, Consumed.with(Serdes.String(), Serdes.Long()));


        regionMetrics.join(userRegions,
                (lk, rk) -> lk,
                (regionValue, metricValue) -> regionValue + "/" + metricValue
        ).to(outputTopicName);

        KTable<String, String> outputTopic = builder.table(outputTopicName, Materialized.as(storeName));

        return builder.build();
    }

    static Topology kStreamToGlobalKTableLeftJoin(String inputTopic1,
                                                  String inputTopic2,
                                                  String outputTopicName,
                                                  String storeName) {

        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, String> userRegions = builder.globalTable(inputTopic1);
        KStream<String, Long> regionMetrics = builder.stream(inputTopic2, Consumed.with(Serdes.String(), Serdes.Long()));

        regionMetrics.leftJoin(userRegions,
                (lk, rk) -> lk,
                (regionValue, metricValue) -> regionValue + "/" + metricValue
        ).to(outputTopicName);

        KTable<String, String> outputTopic = builder.table(outputTopicName, Materialized.as(storeName));

        return builder.build();
    }

    // Not Supported
    static Topology kStreamToGlobalKTableOuterJoin(String inputTopic1,
                                                   String inputTopic2,
                                                   String outputTopicName,
                                                   String storeName) {

        return new StreamsBuilder().build();
    }

}
