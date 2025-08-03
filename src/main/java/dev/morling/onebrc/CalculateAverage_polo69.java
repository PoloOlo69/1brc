package dev.morling.onebrc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;

class CalculateAverage_polo69 {

    record Measurement(String station, float value) {
        static Measurement of(String value) {
            var s = value.split(";");
            return new Measurement(s[0], Float.parseFloat(s[1]));
        }
    }

    record Evaluation(String station, float min, float mean, float max, long abs_frequency){
        static BiConsumer<Evaluation, Measurement> accumulator = Evaluation::accumulate;
        static BiConsumer<Evaluation, Evaluation> combiner = Evaluation::combine;
        static Supplier<Evaluation> supplier = Evaluation::Empty;
        static Evaluation Empty() { return new Evaluation("", 0.0f, 0.0f, 0.0f, 0L); }
        Evaluation accumulate(Measurement m){
            return new Evaluation( m.station,
                    Math.min( min, m.value ),
                    ( mean * abs_frequency + m.value )/( abs_frequency + 1 ),
                    Math.max( max, m.value ),
                    abs_frequency + 1 );
        }
        Evaluation combine(Evaluation other) {
            return new Evaluation( station.isEmpty() ? other.station : station,
                Math.min( min, other.min ),
                (this.mean * this.abs_frequency + other.mean * other.abs_frequency) / (abs_frequency + other.abs_frequency),
                Math.max( max, other.max),
                abs_frequency + other.abs_frequency );
        }
    }

    private static ArrayList<Measurement> readMeasurements(String path) throws IOException {
        try (var stream = Files.lines(Paths.get(path), StandardCharsets.UTF_8)) {
            return stream.map(Measurement::of)
                    .collect(Collectors.toCollection(ArrayList::new));
        }
    }

    private static Stream<Measurement> readMeasurementsAsStream(String path) throws IOException {
        try (var stream = Files.lines(Paths.get(path), StandardCharsets.UTF_8)) {
            return stream.map(Measurement::of);
        }
    }

    public static void main(String[] args) throws IOException {
        Instant start = Instant.now();
        Executors.newSingleThreadExecutor().execute(run1);
        Instant end = Instant.now();
        System.out.println("Execution time: " + Duration.between(start, end).toMillis());
        System.out.println("Press any key to continue...");
        System.in.read();
    }
    // Measurement reduce(Measurement, BinaryOperator<Measurement>)
    // Optional<Measurement> reduce(BinaryOperator<Measurement>)
    // Object reduce(Object, BiFunction<Object, ? super Measurement, Object>, BinaryOperator<Object>)

    // Double result = givenList.stream()
    // .collect(averagingDouble(String::length));
    public static Runnable run1 = () -> {
        try (var stream = Files.lines(Paths.get("measurements.txt"), StandardCharsets.UTF_8)) {
            stream
                    .map(Measurement::of).limit(100).peek(System.out::println )
                    .collect(toEvaluations()).forEach(System.out::println);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    };

    public static Collector<Measurement, ?, ArrayList<Evaluation>> toEvaluations() {
        return collectingAndThen(
                groupingBy(
                        Measurement::station,
                        Collector.of(
                                Evaluation::Empty,
                                Evaluation::accumulate,
                                Evaluation::combine)),
                map -> new ArrayList<>(map.values()));
    }
}

// ein measuremetn hat maxmimal eine nachkomam stelle.
// 4bits für nachkommastelle = 2^4 = 16 :(
// 3 bits = 2^3 = 8 zahlen, 0 kann man evtl als null darstellen
// 1 bit für vor dem komma
// 2^6 bits = 64 für vor dem komma