package com.example.bigdata;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class HouseStatsApp {
    // POJO classes
    static public class InputScores {
        public String house;
        public String character;
        public String score;
        public String ts;
    }

    static public class HouseStats {
        public String house;
        public long how_many;
        public long sum_score;
        public long no_characters;
    }

    static public class HouseStatsState {
        public String house;
        public long how_many = 0;
        public long sum_score = 0;
        public Set<String> characters = new HashSet<>();
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Two parameters are required: boostrapServer");
            System.exit(0);
        }
        final String boostrapServer = args[0];

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "house-stats-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServer);
        // setting offset reset to earliest so that we can re-run the code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Serde<InputScores> inputScoresSerde = new JsonPOJOSerde<>(InputScores.class, false);
        final Serde<HouseStatsState> houseStatsStateSerde = new JsonPOJOSerde<>(HouseStatsState.class, false);
        final Serde<HouseStats> houseStatsSerde = new JsonPOJOSerde<>(HouseStats.class, false);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, InputScores> scores = builder.stream("kafka-input",
                Consumed.with(Serdes.String(), inputScoresSerde));

        KTable<String, HouseStatsState> stats = scores.groupByKey().
                aggregate(
                        HouseStatsState::new,
                        (aggKey, newValue, aggValue) -> {
                            // TODO: uzupełnij brakujący fragment kodu
                            return aggValue;
                        },
                        Materialized.with(Serdes.String(), houseStatsStateSerde)
                );
        KStream<String, HouseStats> resultStream = stats.toStream().
                map(
                        (key, value) -> {
                            HouseStats finalStats = new HouseStats();
                            finalStats.house = value.house;
                            finalStats.how_many = value.how_many;
                            finalStats.sum_score = value.sum_score;
                            finalStats.no_characters = value.characters.size();
                            return KeyValue.pair(key, finalStats);
                        }
                );
        // write to the result topic
        resultStream.to("kafka-output", Produced.with(Serdes.String(), houseStatsSerde));

        Topology topology = builder.build();

        try (KafkaStreams streams = new KafkaStreams(topology, props)) {
            streams.setStateListener((newState, oldState) -> {
                System.out.println("ZMIANA STANU: " + oldState + " -> " + newState);
            });
            final CountDownLatch latch = new CountDownLatch(1);
            // attach shutdown handler to catch control-c
            Runtime.getRuntime().addShutdownHook(new Thread("streams-pipe-shutdown-hook") {
                @Override
                public void run() {
                    streams.close();
                    latch.countDown();
                }
            });
            try {
                streams.start();
                latch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
        }
        System.exit(0);
    }
}

