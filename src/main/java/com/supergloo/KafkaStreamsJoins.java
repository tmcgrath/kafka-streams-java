package com.supergloo;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

public class KafkaStreamsJoins {


    public static void main(String[] args) throws Exception {

//        if (args.length < 1) {
//            throw new IllegalArgumentException("This program takes one argument: the path to an environment configuration file.");
//        }

//        final DynamicOutputTopic instance = new DynamicOutputTopic();
//        final Properties envProps = instance.loadEnvProperties(args[0]);
//        final Properties streamProps = instance.buildStreamsProperties(envProps);
//        final Topology topology = instance.buildTopology(envProps);
//
//        instance.createTopics(envProps);
//
//        final KafkaStreams streams = new KafkaStreams(topology, streamProps);
//        final CountDownLatch latch = new CountDownLatch(1);
//
//        // Attach shutdown handler to catch Control-C.
//        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
//            @Override
//            public void run() {
////                streams.close(Duration.ofSeconds(5));
//                latch.countDown();
//            }
//        });
//
//        try {
//            streams.start();
//            latch.await();
//        } catch (Throwable e) {
//            System.exit(1);
//        }
//        System.exit(0);
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
