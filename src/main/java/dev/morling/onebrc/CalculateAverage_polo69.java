package dev.morling.onebrc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.Files.lines;
import static java.util.stream.Collectors.*;

class CalculateAverage_polo69 {

    record Measurement(String station, double value) {
        static Measurement of(String value) {
            var s = value.split(";");
            return new Measurement(s[0], Float.parseFloat(s[1]));
        }
    }

    record Evaluation(String station, double min, double mean, double max, long abs_frequency){
        static BiConsumer<Evaluation, Measurement> accumulator = Evaluation::accumulate;
        static BiConsumer<Evaluation, Evaluation> combiner = Evaluation::combine;
        static Supplier<Evaluation> supplier = Evaluation::Empty;
        static Evaluation Empty() { return new Evaluation("", 0.0, 0.0, 0.0, 0L); }
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
        var start = Instant.now();
        var f = Executors.newSingleThreadExecutor().submit(run2);
        var end = Instant.now();
        System.out.println("Execution time: " + Duration.between(start, end).toMillis());
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
    record Pair<A,B> (A a, B b) { }

    static Runnable run2 = () -> {
        try ( var stream = lines(Paths.get("measurements.txt"), StandardCharsets.UTF_8) ) {
            stream.parallel().map(Measurement::of).collect(Collectors.collectingAndThen(

                    Collectors.groupingByConcurrent(
                            Measurement::station,
                            Collectors.teeing(
                                    Collectors.teeing( // Pair<Double, Double>(min, max)
                                            Collectors.minBy(Comparator.comparingDouble(Measurement::value)), // min
                                            Collectors.maxBy(Comparator.comparingDouble(Measurement::value)), // max
                                            (min, max) -> new Pair<>(
                                                    min.map(Measurement::value).orElse(Double.NaN),
                                                    max.map(Measurement::value).orElse(Double.NaN)
                                            )
                                    ),
                                    Collectors.teeing( // Pair<Double, Long>(avg, abs)
                                            Collectors.averagingDouble(Measurement::value), // avg
                                            Collectors.counting(), // abs
                                            Pair::new
                                    ),
                                    Pair::new
                            )
                    ),
                    map -> map.entrySet().stream().parallel()
                            .map(entry -> {
                                var min_max = entry.getValue().a;
                                var avg_abs = entry.getValue().b;
                                return new Evaluation(entry.getKey(), min_max.a, min_max.b, avg_abs.a, avg_abs.b);
                            }).collect(Collectors.toCollection(ArrayList::new)))
            ).forEach(System.out::println);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    };
    // numbers.stream().collect(teeing(
    //  minBy(Integer::compareTo), // The first collector
    //  maxBy(Integer::compareTo), // The second collector
    //  (min, max) -> // Receives the result from those collectors and combines them
    //));

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