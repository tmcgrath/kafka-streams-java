package com.supergloo;

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class KafkaStreamsJoinsTest {

    private final static String inputTopicOne = "input-topic-1";
    private final static String inputTopicTwo = "input-topic-2";
    private final static String outputTopic = "output-topic";

    private final static String stateStore = "saved-state";

    private final Serde<String> stringSerde = new Serdes.StringSerde();
    private final Serde<Long> longSerde = new Serdes.LongSerde();

    private static Properties config;
    // input-topic-1
    final List<KeyValue<String, String>> userRegions = Arrays.asList(
            new KeyValue<>("sensor-1", "MN"),
            new KeyValue<>("sensor-2", "WI"),
            new KeyValue<>("sensor-3-in-topic-one", "IL")
    );
    // input-topic-2
    final List<KeyValue<String, Long>> sensorMetric = Arrays.asList(
            new KeyValue<>("sensor-1", 99L),
            new KeyValue<>("sensor-2", 1L),
            new KeyValue<>("sensor-99-in-topic-two", 1L),
            new KeyValue<>("sensor-100-in-topic-two", 100L)
    );

    @BeforeClass
    public static void setConfig() throws InterruptedException {
        config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "testing");

        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "testing:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }

    // -------  KTable to KTable Joins ------------ //
    @Test
    public void shouldKTableToKTableInnerJoin() {

        try (final TopologyTestDriver testDriver =
                     new TopologyTestDriver(
                             KafkaStreamsJoins.kTableToKTableJoin(inputTopicOne, inputTopicTwo, stateStore),
                             config
                     )) {

            final TestInputTopic<String, String> testInputTopic = testDriver.createInputTopic(inputTopicOne, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, Long> testInputTopicTwo = testDriver.createInputTopic(inputTopicTwo, new StringSerializer(), new LongSerializer());

            testInputTopic.pipeKeyValueList(userRegions);
            testInputTopicTwo.pipeKeyValueList(sensorMetric);

            KeyValueStore<String, String> store = testDriver.getKeyValueStore(stateStore);

            assertThat(store.get("sensor-1"), equalTo("MN/99"));
            assertNull(store.get("sensor-3-in-topic-one"));
            assertNull(store.get("sensor-99-in-topic-two"));
        }
    }

    @Test
    public void shouldKTableToKTableLeftJoin() {

        try (final TopologyTestDriver testDriver =
                     new TopologyTestDriver(KafkaStreamsJoins.kTableToKTableLeftJoin(inputTopicOne, inputTopicTwo, stateStore),
                             config)) {

            final TestInputTopic<String, String> testInputTopic = testDriver.createInputTopic(inputTopicOne, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, Long> testInputTopicTwo = testDriver.createInputTopic(inputTopicTwo, new StringSerializer(), new LongSerializer());

            testInputTopic.pipeKeyValueList(userRegions);
            testInputTopicTwo.pipeKeyValueList(sensorMetric);

            KeyValueStore<String, String> store = testDriver.getKeyValueStore(stateStore);

            assertThat(store.get("sensor-1"), equalTo("MN/99"));
            assertThat(store.get("sensor-3-in-topic-one"), equalTo("IL/null"));
            assertNull(store.get("sensor-99-in-topic-two"));
            assertNull(store.get("sensor-100-in-topic-two"));
        }
    }

    @Test
    public void shouldKTableToKTableOuterJoin() {

        try (final TopologyTestDriver testDriver =
                     new TopologyTestDriver(KafkaStreamsJoins.kTableToKTableOuterJoin(inputTopicOne, inputTopicTwo, stateStore),
                             config)) {

            final TestInputTopic<String, String> testInputTopic = testDriver.createInputTopic(inputTopicOne, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, Long> testInputTopicTwo = testDriver.createInputTopic(inputTopicTwo, new StringSerializer(), new LongSerializer());

            testInputTopic.pipeKeyValueList(userRegions);
            testInputTopicTwo.pipeKeyValueList(sensorMetric);

            KeyValueStore<String, String> store = testDriver.getKeyValueStore(stateStore);


            assertThat(store.get("sensor-1"), equalTo("MN/99"));
            assertThat(store.get("sensor-3-in-topic-one"), equalTo("IL/null"));

            assertThat(store.get("sensor-99-in-topic-two"), equalTo("null/1"));
            assertThat(store.get("sensor-100-in-topic-two"), equalTo("null/100"));

        }
    }

    // -------  KStream to KTable Joins ------------ //

    @Test
    public void shouldKStreamToKTableInnerJoin() {

        try (final TopologyTestDriver testDriver =
                     new TopologyTestDriver(
                             KafkaStreamsJoins.kStreamToKTableJoin(
                                     inputTopicOne, inputTopicTwo, outputTopic, stateStore
                             ),
                             config)) {

            final TestInputTopic<String, String> testInputTopic = testDriver.createInputTopic(inputTopicOne, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, Long> testInputTopicTwo = testDriver.createInputTopic(inputTopicTwo, new StringSerializer(), new LongSerializer());

            testInputTopic.pipeKeyValueList(userRegions);
            testInputTopicTwo.pipeKeyValueList(sensorMetric);

            KeyValueStore<String, String> store = testDriver.getKeyValueStore(stateStore);

            // Perform tests
            assertThat(store.get("sensor-1"), equalTo("99/MN")); // v,k compared with above
            assertNull(store.get("sensor-3-in-topic-one"));
            assertNull(store.get("sensor-99-in-topic-two"));
            assertNull(store.get("sensor-100-in-topic-two"));
        }
    }

    @Test
    public void shouldKStreamToKTableLeftJoin() {

        try (final TopologyTestDriver testDriver =
                     new TopologyTestDriver(
                             KafkaStreamsJoins.kStreamToKTableLeftJoin(
                                     inputTopicOne, inputTopicTwo, outputTopic, stateStore
                             ),
                             config)) {

            final TestInputTopic<String, String> testInputTopic = testDriver.createInputTopic(inputTopicOne, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, Long> testInputTopicTwo = testDriver.createInputTopic(inputTopicTwo, new StringSerializer(), new LongSerializer());

            testInputTopic.pipeKeyValueList(userRegions);
            testInputTopicTwo.pipeKeyValueList(sensorMetric);

            KeyValueStore<String, String> store = testDriver.getKeyValueStore(stateStore);

            // Perform tests
            assertThat(store.get("sensor-1"), equalTo("99/MN")); // v,k compared with above
            assertNull(store.get("sensor-3-in-topic-one"));
            assertThat(store.get("sensor-99-in-topic-two"), equalTo("1/null"));
            assertThat(store.get("sensor-100-in-topic-two"), equalTo("100/null"));
        }
    }

    // KStream to KTable outer join is not supported

    // -------  KStream to KStream Joins ------------ //
    @Test
    public void shouldKStreamToKStreamJoin() {

        try (final TopologyTestDriver testDriver =
                     new TopologyTestDriver(
                             KafkaStreamsJoins.kStreamToKStreamJoin(
                                     inputTopicOne, inputTopicTwo, outputTopic, stateStore
                             ),
                             config)) {

            final TestInputTopic<String, String> testInputTopic = testDriver.createInputTopic(inputTopicOne, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, Long> testInputTopicTwo = testDriver.createInputTopic(inputTopicTwo, new StringSerializer(), new LongSerializer());

            testInputTopic.pipeKeyValueList(userRegions);
            testInputTopicTwo.pipeKeyValueList(sensorMetric);

            KeyValueStore<String, String> store = testDriver.getKeyValueStore(stateStore);

            // Perform tests
            assertThat(store.get("sensor-1"), equalTo("99/MN")); // v,k compared with above
            assertNull(store.get("sensor-3-in-topic-one"));
            assertNull(store.get("sensor-99-in-topic-two"));
        }
    }

    @Test
    public void shouldKStreamToKStreamLeftJoin() {

        try (final TopologyTestDriver testDriver =
                     new TopologyTestDriver(
                             KafkaStreamsJoins.kStreamToKStreamLeftJoin(
                                     inputTopicOne, inputTopicTwo, outputTopic, stateStore
                             ),
                             config)) {

            final TestInputTopic<String, String> testInputTopic = testDriver.createInputTopic(inputTopicOne, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, Long> testInputTopicTwo = testDriver.createInputTopic(inputTopicTwo, new StringSerializer(), new LongSerializer());

            testInputTopic.pipeKeyValueList(userRegions);
            testInputTopicTwo.pipeKeyValueList(sensorMetric);

            KeyValueStore<String, String> store = testDriver.getKeyValueStore(stateStore);

            // Perform tests
            assertThat(store.get("sensor-1"), equalTo("99/MN"));
            assertNull(store.get("sensor-3-in-topic-one"));
            assertThat(store.get("sensor-99-in-topic-two"), equalTo("1/null"));
            assertThat(store.get("sensor-100-in-topic-two"), equalTo("100/null"));

        }
    }

    @Test
    public void shouldKStreamToKStreamOuterJoin() {

        try (final TopologyTestDriver testDriver =
                     new TopologyTestDriver(
                             KafkaStreamsJoins.kStreamToKStreamOuterJoin(
                                     inputTopicOne, inputTopicTwo, outputTopic, stateStore
                             ),
                             config)) {

            final TestInputTopic<String, String> testInputTopic = testDriver.createInputTopic(inputTopicOne, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, Long> testInputTopicTwo = testDriver.createInputTopic(inputTopicTwo, new StringSerializer(), new LongSerializer());

            testInputTopic.pipeKeyValueList(userRegions);
            testInputTopicTwo.pipeKeyValueList(sensorMetric);

            KeyValueStore<String, String> store = testDriver.getKeyValueStore(stateStore);

            // Perform tests
            assertThat(store.get("sensor-1"), equalTo("99/MN"));
            assertThat(store.get("sensor-3-in-topic-one"), equalTo("null/IL"));
            assertThat(store.get("sensor-99-in-topic-two"), equalTo("1/null"));
            assertThat(store.get("sensor-100-in-topic-two"), equalTo("100/null"));
        }
    }

    // -------  KStream to GlobalKTable Joins ------------ //
    // Expect results are the same as KStream to KTable examples

    @Test
    public void shouldKStreamToGlobalKTableJoin() {

        try (final TopologyTestDriver testDriver =
                     new TopologyTestDriver(
                             KafkaStreamsJoins.kStreamToGlobalKTableJoin(
                                     inputTopicOne, inputTopicTwo, outputTopic, stateStore
                             ),
                             config)) {

            final TestInputTopic<String, String> testInputTopic = testDriver.createInputTopic(inputTopicOne, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, Long> testInputTopicTwo = testDriver.createInputTopic(inputTopicTwo, new StringSerializer(), new LongSerializer());

            testInputTopic.pipeKeyValueList(userRegions);
            testInputTopicTwo.pipeKeyValueList(sensorMetric);

            KeyValueStore<String, String> store = testDriver.getKeyValueStore(stateStore);

            // Perform tests
            assertThat(store.get("sensor-1"), equalTo("99/MN"));
            assertNull(store.get("sensor-3-in-topic-one"));
            assertNull(store.get("sensor-99-in-topic-two"));

        }
    }

    @Test
    public void shouldKStreamToGlobalKTableLeftJoin() {

        try (final TopologyTestDriver testDriver =
                     new TopologyTestDriver(
                             KafkaStreamsJoins.kStreamToGlobalKTableLeftJoin(
                                     inputTopicOne, inputTopicTwo, outputTopic, stateStore
                             ),
                             config)) {

            final TestInputTopic<String, String> testInputTopic = testDriver.createInputTopic(inputTopicOne, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, Long> testInputTopicTwo = testDriver.createInputTopic(inputTopicTwo, new StringSerializer(), new LongSerializer());

            testInputTopic.pipeKeyValueList(userRegions);
            testInputTopicTwo.pipeKeyValueList(sensorMetric);

            KeyValueStore<String, String> store = testDriver.getKeyValueStore(stateStore);

            // Perform tests
            assertThat(store.get("sensor-1"), equalTo("99/MN"));
            assertNull(store.get("sensor-3-in-topic-one"));
            assertThat(store.get("sensor-99-in-topic-two"), equalTo("1/null"));
            assertThat(store.get("sensor-100-in-topic-two"), equalTo("100/null"));

        }
    }
    // KStream to GlobalKTable outer join is not supported
}
