/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
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

import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.Files.lines;

class CalculateAverage_polo69 {

    value record Measurement(String station, double value) {

    static Measurement of(String value) {
        var idx = value.indexOf(';');
        return new Measurement(value.substring(0,idx),
                Double.parseDouble(value.substring(idx+1)));
    }

    }

    record Evaluation(String station, double min, double mean, double max, long abs_frequency){
    static Evaluation Empty() {
        return new Evaluation("", 0.0, 0.0, 0.0, 0L);
    }
    Evaluation accumulate(Measurement m) {
        return new Evaluation(m.station,
                Math.min(min, m.value),
                (mean * abs_frequency + m.value) / (abs_frequency + 1),
                Math.max(max, m.value),
                abs_frequency + 1);
    }
    Evaluation combine(Evaluation other) {
        return new Evaluation(station.isEmpty() ? other.station : station,
                Math.min(min, other.min),
                (this.mean * this.abs_frequency + other.mean * other.abs_frequency) / (abs_frequency + other.abs_frequency),
                Math.max(max, other.max),
                abs_frequency + other.abs_frequency);}
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
        var runs = List.of(run);
        var durations = new ArrayList<>();
        var results = new ArrayList<>();

        for (Callable<ArrayList<Evaluation>> arrayListCallable : runs) {
            Instant start = Instant.now();
            ArrayList<Evaluation> result = arrayListCallable.call();
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
        System.out.println(results.get(0));
    }

    value record Pair<A,B>(A a, B b){}

    static Callable<ArrayList<Evaluation>> run = () -> {
        try ( var stream = lines(Paths.get("measurements.txt"), StandardCharsets.UTF_8) ) {
            return stream.map(Measurement::of).collect(Collectors.collectingAndThen(

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
        } catch (Exception e){throw new RuntimeException(e);}
    };
}




