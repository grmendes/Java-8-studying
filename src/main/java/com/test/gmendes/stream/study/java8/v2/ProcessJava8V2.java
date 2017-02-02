package com.test.gmendes.stream.study.java8.v2;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import static java.util.stream.Collectors.toMap;

/**
 * This class contains the Java 8 based implementation of the project. The main goal is to process a batch of files,
 * generating SQL insert strings, as proof of concept.
 * <p>
 * The main goal is to use as many Java 8 new features as possible into this implementation.
 * <p>
 * This class holds the second version of the Java 8's implementation. Plenty of improvements where introduced, in order
 * to minimize iterations and terminal operations. Lots of unnecessary code was removed. Please note that I'm still
 * learning to use Streams and Lambdas, so some optimization may be available into this code.
 *
 * @author grmendes
 * @version 2
 */
public class ProcessJava8V2 {

    private final String path;

    public ProcessJava8V2(String path) {
        this.path = path;
    }

    /**
     * Reads all file lines.
     * <p>
     * Based on Java 8's new Files API default file reading code.
     *
     * @param path     Path to the file.
     * @param filename Name of the file.
     * @return List of String with all file's lines.
     */
    public static List<String> readFile(String path, String filename) {
        try {
            return Files.readAllLines(Paths.get(path, filename), ISO_8859_1);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return Collections.emptyList();
    }

    /**
     * Converts the received list into a map, splitting it by the predicate informed. For each sublist generated,
     * removes its first element and uses it as the Map's key.
     * <p>
     * Uses a reduce operation to split the input list's elements into sublists.
     * <p>
     * Based on <a href='http://stackoverflow.com/questions/29095967/splitting-list-into-sublists-along-elements'>this Stackoverflow</a>.
     * <p>
     * Example:
     * <li>input list = ["Header", "1", "2", "", "Header2", "3"]</li>
     * <li>input predicate = String::isEmpty</li>
     * <li>output map = [{"Header", ["1", "2"]}, {"Header2", ["3"]}]</li>
     *
     * @param list List to be converted.
     * @param sep  Predicate indicating the separator in use.
     * @param <T>  Type used by the input list, the key and the list's elements of the output map and the predicate's
     *             evaluation.
     * @return Map with a T key and a List of T as value.
     */
    public static <T> Map<T, List<T>> listToMap(List<T> list, Predicate<T> sep) {
        return list.stream().reduce(new ArrayList<List<T>>(),
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
                }).stream().filter(l -> !l.isEmpty())
                .collect(toMap(l -> l.remove(0), l -> l));
    }

    /**
     * Entry point to start processing for testing. Generates a list of SQL inserts, based on the files processed.
     *
     * @return List of the generated inserts.
     */
    public List<String> execute() {

        final Map<String, List<String>> mapLinesPerTable = listToMap(readFile(path, LAYOUT_FILE_BASE_NAME),
                String::isEmpty);

        return mapLinesPerTable.entrySet().parallelStream().flatMap(this::process).collect(toList());
    }

    /**
     * Validates the quality of input. This step is not strictly needed, but simulates a real scenario.
     * The data to process contains a file named layout.txt, which contains all tables, tables' columns and positional
     * information for data extraction. Also, there's a <TABLE_NAME>_layout.txt file with specific table columns
     * and positional information for data extraction. Both general layout and table specific layout files must contain
     * the same information. This method validates if both information are the same.
     * <p>
     * This method generates two ordered strings, one containing the information from the general layout file (entry
     * param) and the other containing the information from the table's specific layout file.
     *
     * @param entry Map.Entry containing the table name as key and the general layout's content as value.
     * @throws RuntimeException if results are not the same. Not supposed to occur.
     */
    private void validate(Map.Entry<String, List<String>> entry) {
        String layout = entry.getValue().stream().sorted().collect(joining(SEPARATOR));
        String file = readFile(path, entry.getKey() + "_" + LAYOUT_FILE_BASE_NAME).stream().sorted()
                .collect(joining(SEPARATOR));
        if (!layout.equals(file)) {
            throw new RuntimeException(entry.getKey());
        }
    }

    /**
     * Receives a table's layout data, gets its data and generates inserts to this table with the data.
     *
     * @param entry Map.Entry with the table's name as key and List of table's layout information as value.
     * @return Stream of String containing a SQL insert generated by the code.
     */
    private Stream<String> process(Map.Entry<String, List<String>> entry) {
        validate(entry);

        final List<String> layout = entry.getValue();
        // Remove header line from layout
        layout.remove(LAYOUT_HEADER);

        String tableName = entry.getKey();
        final List<String> fileData = readFile(path, tableName + TXT_EXTENSION);
        final Supplier<Stream<String>> columnSupplier =
                () -> layout.stream().map(layoutLine -> layoutLine.split(SEPARATOR)[0]).sorted();

        final String baseInsertText =
                columnSupplier.get().collect(joining(SEPARATOR, "INSERT INTO " + tableName + " (", ") VALUES ("));

        return fileData.parallelStream().map(fileLine -> {
            final Map<String, String> data = new HashMap<>();
            layout.forEach(layoutLine -> {
                final String[] layoutInfo = layoutLine.split(SEPARATOR);
                // layoutInfo[0] = Column name
                // layoutInfo[2] = Start position of information
                // layoutInfo[3] = End position of information
                // Next line maps the column value to its content
                data.put(layoutInfo[0], fileLine.substring(Integer.parseInt(layoutInfo[2]),
                        Integer.parseInt(layoutInfo[3])));
            });
            return generateInsert(data, columnSupplier, baseInsertText);
        });
    }

    /**
     * Generates a SQL insert string based on parameters.
     *
     * @param data           Map with a column name as key and its data as value.
     * @param columnSupplier Supplier for a Stream of columns to the insert.
     * @param baseInsertText The base insert text, which contains all information before the values itself.
     * @return String with the SQL insert generated.
     */
    private String generateInsert(Map<String, String> data, Supplier<Stream<String>> columnSupplier, String baseInsertText) {
        return columnSupplier.get()
                .map(column -> {
                    String value = data.get(column);
                    return !value.trim().isEmpty() ? value : NULL;
                })
                .collect(joining(SEPARATOR, baseInsertText, ");"));
    }
}
