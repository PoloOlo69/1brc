package dev.morling.onebrc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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

    public static void main(String... args) throws Exception {
        List<Callable<ArrayList<Evaluation>>> runs = List.of(run);
        List<Long> durations = new ArrayList<>();
        List<ArrayList<Evaluation>> results = new ArrayList<>();

        for (Callable<ArrayList<Evaluation>> arrayListCallable : runs) {
            Instant start = Instant.now();
            ArrayList<Evaluation> result = arrayListCallable.call(); // direkt ausführen (sequenziell)
            Instant end = Instant.now();
            long duration = Duration.between(start, end).toMillis();
            durations.add(duration);
            results.add(result);
        }

        // Zeiten ausgeben
        System.out.println("\nExecution times:");
        for (int i = 0; i < durations.size(); i++) {
            System.out.printf("run%d: %d ms%n", i + 1, durations.get(i));
        }

        // Optionale Ergebnisanzeige von run1
        System.out.println("\nSample results from run1:");
        results.get(0).forEach(System.out::println);
    }

    record Pair<A,B> (A a, B b) { }

    static Callable<ArrayList<Evaluation>> run3 = () -> {
        try (var stream = Files.lines(Paths.get("measurements.txt"), StandardCharsets.UTF_8)) {
            return stream.parallel().collect(Collectors.collectingAndThen(
                    Collectors.groupingByConcurrent(
                            line -> line.substring(0, line.indexOf(';')), // group by station name
                            Collectors.teeing(
                                    Collectors.teeing( // min/max
                                            Collectors.mapping(
                                                    line -> {
                                                        int sep = line.indexOf(';');
                                                        return Double.parseDouble(line.substring(sep + 1));
                                                    },
                                                    Collectors.minBy(Double::compare)
                                            ),
                                            Collectors.mapping(
                                                    line -> {
                                                        int sep = line.indexOf(';');
                                                        return Double.parseDouble(line.substring(sep + 1));
                                                    },
                                                    Collectors.maxBy(Double::compare)
                                            ),
                                            (min, max) -> new Pair<>(
                                                    min.orElse(Double.NaN),
                                                    max.orElse(Double.NaN)
                                            )
                                    ),
                                    Collectors.teeing( // avg + count
                                            Collectors.averagingDouble(line -> {
                                                int sep = line.indexOf(';');
                                                return Double.parseDouble(line.substring(sep + 1));
                                            }),
                                            Collectors.counting(),
                                            Pair::new
                                    ),
                                    Pair::new
                            )
                    ),
                    map -> map.entrySet().stream().parallel()
                            .map(entry -> {
                                var minMax = entry.getValue().a;
                                var avgCount = entry.getValue().b;
                                return new Evaluation(entry.getKey(), minMax.a, avgCount.a, minMax.b, avgCount.b);
                            })
                            .collect(Collectors.toCollection(ArrayList::new))
            ));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    };

    static Callable<ArrayList<Evaluation>> run2 = () -> {
        try (var stream = lines(Paths.get("measurements.txt"), StandardCharsets.UTF_8).parallel()) {
            return stream
                    .map(Measurement::of)
                    .collect(Collectors.collectingAndThen(

                            Collectors.groupingByConcurrent(
                                    Measurement::station,
                                    Collectors.teeing(
                                            // 1. Collector für min & max
                                            Collectors.teeing(
                                                    Collectors.minBy(Comparator.comparingDouble(Measurement::value)),
                                                    Collectors.maxBy(Comparator.comparingDouble(Measurement::value)),
                                                    (minOpt, maxOpt) -> new double[] {
                                                            minOpt.map(Measurement::value).orElse(Double.NaN),
                                                            maxOpt.map(Measurement::value).orElse(Double.NaN)
                                                    }
                                            ),

                                            // 2. Collector für mean & count
                                            Collectors.teeing(
                                                    Collectors.averagingDouble(Measurement::value),
                                                    Collectors.counting(),
                                                    (mean, count) -> new Object[] { mean, count }
                                            ),

                                            // Combine beide Sammel-Ergebnisse zu Evaluation
                                            (minMax, meanCount) -> new Object[] { minMax, meanCount }
                                    )
                            ),

                            // map<K, Object[]> → List<Evaluation>
                            map -> {
                                ArrayList<Evaluation> result = new ArrayList<>(map.size());
                                for (var entry : map.entrySet()) {
                                    String station = entry.getKey();
                                    var combined = entry.getValue();
                                    var minMax = (double[]) combined[0];
                                    var meanCount = (Object[]) combined[1];
                                    result.add(new Evaluation(
                                            station,
                                            minMax[0],
                                            (double) meanCount[0],
                                            minMax[1],
                                            (long) meanCount[1]
                                    ));
                                }
                                return result;
                            }
                    ));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    };

    static Callable<ArrayList<Evaluation>> run = () -> {
        try ( var stream = lines(Paths.get("measurements.txt"), StandardCharsets.UTF_8) ) {
            return stream.parallel().map(Measurement::of).collect(Collectors.collectingAndThen(

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
            );
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