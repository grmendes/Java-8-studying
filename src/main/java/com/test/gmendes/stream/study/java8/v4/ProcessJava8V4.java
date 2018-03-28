package com.test.gmendes.stream.study.java8.v4;

import one.util.streamex.StreamEx;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.test.gmendes.stream.study.Constants.LAYOUT_FILE_BASE_NAME;
import static com.test.gmendes.stream.study.Constants.LAYOUT_HEADER;
import static com.test.gmendes.stream.study.Constants.NULL;
import static com.test.gmendes.stream.study.Constants.SEPARATOR;
import static com.test.gmendes.stream.study.Constants.TXT_EXTENSION;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * This class contains the Java 8 based implementation of the project. The main goal is to process a batch of files,
 * generating SQL insert strings, as proof of concept.
 * <p>
 * The main goal is to use as many Java 8 new features as possible into this implementation.
 * <p>
 * This class holds the third version of the Java 8's implementation. Plenty of improvements where introduced, in order
 * to reduce aggressively the amount of terminal operations. This version basically uses one only Stream from file
 * reading to final Collect for output. Some places still contains terminal operations inside the pipeline, because I
 * was not able to remove them at all.
 *
 * @author grmendes
 * @version 4
 */
public class ProcessJava8V4 {

    private final String path;

    public ProcessJava8V4(String path) {
        this.path = path;
    }

    public static <T> Stream<List<T>> splitList4(Stream<T> list, Predicate<T> sep) {
        StreamEx<T> streamEx = StreamEx.of(list);
        final AtomicInteger count = new AtomicInteger();
        List<List<AbstractMap.SimpleEntry<Integer, T>>> lists = streamEx.peek(elem -> {
            if (sep.test(elem)) {
                count.incrementAndGet();
            }
        }).filter(sep.negate())
                .map(elem -> new AbstractMap.SimpleEntry<>(count.get(), elem))
                .groupRuns((a, b) -> a.getKey().equals(b.getKey())).toList();

        // TODO testar implementaçao com LinkedList
        return list.reduce(new ArrayList<List<T>>(),
                (l, elem) -> {
                    if (l.isEmpty()) {
                        l.add(new ArrayList<>());
                    }
                    if (sep.test(elem)) {
                        l.add(new ArrayList<>());
                    } else {
                        l.get(l.size() - 1).add(elem);
                    }
                    return l;
                },
                (l1, l2) -> {
                    l1.addAll(l2);
                    return l1;
                }).parallelStream().filter(l -> !l.isEmpty());
    }

    /**
     * Entry point to start processing for testing. Generates a list of SQL inserts, based on the files processed.
     *
     * @return List of the generated inserts.
     */
    public List<String> execute() {
        return splitList(readFile(path, LAYOUT_FILE_BASE_NAME), String::isEmpty).flatMap(this::process)
                .collect(toList());
    }

    /**
     * Converts the received stream into a Stream of lists, splitting it by the predicate informed.
     *
     * @param list List to be converted.
     * @param sep  Predicate indicating the separator in use.
     * @param <T>  Type used by the input list, the key and the list's elements of the output map and the predicate's
     *             evaluation.
     * @return Stream with a List of T elements.
     */
    public static <T> Stream<List<T>> splitList(Stream<T> list, Predicate<T> sep) {
        return list.reduce(new ArrayList<List<T>>(),
                (l, elem) -> {
                    if (l.isEmpty()) {
                        l.add(new ArrayList<>());
                    }
                    if (sep.test(elem)) {
                        l.add(new ArrayList<>());
                    } else {
                        l.get(l.size() - 1).add(elem);
                    }
                    return l;
                },
                (l1, l2) -> {
                    l1.addAll(l2);
                    return l1;
                }).parallelStream().filter(l -> !l.isEmpty());
    }

    /**
     * Reads all file lines.
     * <p>
     * Based on Java 8's new Files API file reading code to stream directly.
     *
     * @param path     Path to the file.
     * @param filename Name of the file.
     * @return Stream of String with all file's lines.
     */
    public static Stream<String> readFile(String path, String filename) {
        try {
            return Files.lines(Paths.get(path, filename), ISO_8859_1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Stream.empty();
    }

    /**
     * Receives a table's layout data, gets its data and generates inserts to this table with the data.
     *
     * @param layoutList List of table's layout information, where the first element is the table's name.
     * @return Stream of String containing a SQL insert generated by the code.
     */
    private Stream<String> process(final List<String> layoutList) {
        final String tableName = layoutList.remove(0);
        validate(tableName, layoutList);
        // Remove header line from layout
        layoutList.remove(LAYOUT_HEADER);

        final Supplier<Stream<String[]>> layoutInfoSupplier =
                () -> layoutList.stream().map(layoutLine -> layoutLine.split(SEPARATOR))
                        .sorted(Comparator.comparing(elem -> elem[0]));

        // layoutInfo[0] = Column name
        final String baseInsertText = layoutInfoSupplier.get().map(layoutInfo -> layoutInfo[0])
                .collect(joining(SEPARATOR, "INSERT INTO " + tableName + " (", ") VALUES ("));

        return readFile(path, tableName + TXT_EXTENSION).parallel()
                .map(fileLine -> layoutInfoSupplier.get()
                        .map(layoutInfo -> {
                            // layoutInfo[2] = Start position of information
                            // layoutInfo[3] = End position of information
                            // Next line maps the column value to its content
                            String value = fileLine.substring(Integer.parseInt(layoutInfo[2]),
                                    Integer.parseInt(layoutInfo[3]));
                            return !value.trim().isEmpty() ? value : NULL;
                        }).collect(joining(SEPARATOR, baseInsertText, ");"))
                );
    }

    /**
     * Validates the quality of input. This step is not strictly needed, but simulates a real scenario.
     * The data to process contains a file named layout.txt, which contains all tables, tables' columns and positional
     * information for data extraction. Also, there's a <TABLE_NAME>_layout.txt file with specific table columns
     * and positional information for data extraction. Both general layout and table specific layout files must contain
     * the same information. This method validates if both information are the same.
     * <p>
     * This method receives a list with the information from the general layout file (layoutList param) and
     * reads a list containing the information from the table's specific layout file. Then, it compares both with
     * List.containsAll method.
     *
     * @param tableName  the table name.
     * @param layoutList the general layout's content.
     * @throws RuntimeException if results are not the same. Not supposed to occur.
     */
    private void validate(String tableName, List<String> layoutList) {
        List<String> fileList = readFile(path, tableName + "_" + LAYOUT_FILE_BASE_NAME).collect(toList());
        // If layoutList contains all elements inside fileList and fileList contains all elements inside layoutList,
        // then both lists are equals.
        if (!layoutList.containsAll(fileList) || !fileList.containsAll(layoutList)) {
            throw new RuntimeException(tableName);
        }
    }
}
