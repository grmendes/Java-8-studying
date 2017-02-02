package com.test.gmendes.stream.study.main;

import com.test.gmendes.stream.study.java7.v1.ProcessJava7V1;
import com.test.gmendes.stream.study.java7.v2.ListToMapJava7Tests;
import com.test.gmendes.stream.study.java7.v2.ProcessJava7V2;
import com.test.gmendes.stream.study.java8.v1.ProcessJava8V1;
import com.test.gmendes.stream.study.java8.v2.ListToMapJava8Tests;
import com.test.gmendes.stream.study.java8.v2.ProcessJava8V2;
import com.test.gmendes.stream.study.java8.v3.ProcessJava8V3;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.security.InvalidParameterException;
import java.util.concurrent.TimeUnit;

import static com.test.gmendes.stream.study.Constants.LAYOUT_FILE_BASE_NAME;

/**
 * Main class for tests. It uses JMH suit to benchmark the executions.
 *
 * @author grmendes
 */
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class Main {

    private static String path;

    static {
        path = System.getProperty("path");
        if (path == null || path.isEmpty()) {
            throw new InvalidParameterException("This program should receive a path to a directory as argument. Please use -Dpath='/path/to/Stream_Study/src/main/resources/Sigtap/' argument to java command.");
        }
        if (!path.endsWith(File.separator)) {
            path = path + File.separator;
        }
    }

    public static void main(String... args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Main.class.getSimpleName())
                .resultFormat(ResultFormatType.TEXT)
                .shouldDoGC(true)
                .warmupIterations(5)
                .forks(1)
                .threads(1)
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void readFileJava7V1() {
        ProcessJava7V1.readFile(path, LAYOUT_FILE_BASE_NAME);
    }

    @Benchmark
    public void readFileJava7V2() {
        ProcessJava7V2.readFile(path, LAYOUT_FILE_BASE_NAME);
    }

    @Benchmark
    public void readFileJava8V1() {
        ProcessJava8V1.readFile(path, LAYOUT_FILE_BASE_NAME);
    }

    @Benchmark
    public void readFileJava8V2() {
        ProcessJava8V2.readFile(path, LAYOUT_FILE_BASE_NAME);
    }

    @Benchmark
    public void readFileJava8V3() {
        // This test actually does not evaluates the file content, because it returns a stream.
        // It's really important to call close() on end of stream, so the file is closed.
        ProcessJava8V3.readFile(path, LAYOUT_FILE_BASE_NAME).close();
    }

    @Benchmark
    public void listToMap7V1() {
        ProcessJava7V1.listToMap(ProcessJava8V2.readFile(path, LAYOUT_FILE_BASE_NAME));
    }

    @Benchmark
    public void listToMap7V2() {
        ProcessJava7V2.listToMap(ProcessJava8V2.readFile(path, LAYOUT_FILE_BASE_NAME));
    }

    @Benchmark
    public void listToMap8V1() {
        ProcessJava8V1.listToMap(ProcessJava8V2.readFile(path, LAYOUT_FILE_BASE_NAME), String::isEmpty);
    }

    @Benchmark
    public void listToMap8V2() {
        ProcessJava8V2.listToMap(ProcessJava8V2.readFile(path, LAYOUT_FILE_BASE_NAME), String::isEmpty);
    }

    //    @Benchmark
//    public void splitList8V3() {
    // This test actually does not evaluates the file content, because it returns a stream.
    // This test is commented because it's producing lots of exceptions during execution, even calling close()
    // method from Stream.
//        ProcessJava8V3.splitList(ProcessJava8V3.readFile(path, LAYOUT_FILE_BASE_NAME), String::isEmpty).close();
//    }

    @Benchmark
    public void listToMap7Edges() {
        ListToMapJava7Tests.listToMap7Edges(ProcessJava8V2.readFile(path, LAYOUT_FILE_BASE_NAME));
    }

    @Benchmark
    public void listToMap8ForEach() {
        ListToMapJava8Tests.listToMap8ForEach(ProcessJava8V2.readFile(path, LAYOUT_FILE_BASE_NAME), String::isEmpty);
    }

    @Benchmark
    public void listToMap8CollectorOptimized() {
        ListToMapJava8Tests.listToMap8CollectorOptimized(ProcessJava8V2.readFile(path, LAYOUT_FILE_BASE_NAME), String::isEmpty);
    }

    @Benchmark
    public void listToMap8Edges() {
        ListToMapJava8Tests.listToMap8Edges(ProcessJava8V2.readFile(path, LAYOUT_FILE_BASE_NAME), String::isEmpty);
    }

    @Benchmark
    public void ProcessJava7V1() {
        new ProcessJava7V1(path).execute();
    }

    @Benchmark
    public void ProcessJava7V2() {
        new ProcessJava7V2(path).execute();
    }

    @Benchmark
    public void ProcessJava8V1() {
        new ProcessJava8V1(path).execute();
    }

    @Benchmark
    public void ProcessJava8V2() {
        new ProcessJava8V2(path).execute();
    }

    @Benchmark
    public void ProcessJava8V3() {
        new ProcessJava8V3(path).execute();
    }

}
