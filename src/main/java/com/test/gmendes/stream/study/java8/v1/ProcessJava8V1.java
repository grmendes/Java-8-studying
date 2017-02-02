package com.test.gmendes.stream.study.java8.v1;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static com.test.gmendes.stream.study.Constants.LAYOUT_FILE_BASE_NAME;
import static com.test.gmendes.stream.study.Constants.LAYOUT_HEADER;
import static com.test.gmendes.stream.study.Constants.NULL;
import static com.test.gmendes.stream.study.Constants.SEPARATOR;
import static com.test.gmendes.stream.study.Constants.TXT_EXTENSION;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.util.Collections.singletonList;
import static java.util.Collections.synchronizedList;
import static java.util.Collections.synchronizedMap;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * This class contains the Java 8 based implementation of the project. The main goal is to process a batch of files,
 * generating SQL insert strings, as proof of concept.
 * <p>
 * The main goal is to use as many Java 8 new features as possible into this implementation.
 * <p>
 * This class holds the first version of the code at all, which was also my first contact with Java 8. Please note that
 * lots of codes in here can be (and already was) improved, either for performance or readability.
 *
 * @author grmendes
 * @version 1
 */
public class ProcessJava8V1 {

    private final String path;

    public ProcessJava8V1(String path) {
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
     * Collector to produce a splitted list according to predicate informed.
     *
     * @param sep Predicate to indicate the separator to use.
     * @param <T> Type of the Stream's objects.
     * @return List of List of T, which is a list of splitted lists.
     */
    private static <T> Collector<T, List<List<T>>, List<List<T>>> splitBySeparator(Predicate<T> sep) {
        return Collector.of(() -> synchronizedList(new ArrayList<>(singletonList(
                synchronizedList(new ArrayList<>())))),
                (l, elem) -> {
                    if (sep.test(elem)) {
                        l.add(new ArrayList<>());
                    } else {
                        l.get(l.size() - 1).add(elem);
                    }
                },
                (l1, l2) -> {
                    l1.get(l1.size() - 1).addAll(l2.remove(0));
                    l1.addAll(l2);
                    return l1;
                });
    }

    /**
     * Converts the received list into a map, splitting it by the predicate informed. For each sublist generated,
     * removes its first element and uses it as the Map's key.
     * <p>
     * Uses a special collector to split the list and then uses the default <code>toMap</code> collector to generate
     * the output map.
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
        return synchronizedMap(list.stream().collect(splitBySeparator(sep)).stream().filter(l -> !l.isEmpty())
                .collect(Collectors.toMap(l -> l.get(0), l -> {
                    l.remove(0);
                    return l;
                })));
    }

    /**
     * Entry point to start processing for testing. Generates a list of SQL inserts, based on the files processed.
     *
     * @return List of the generated inserts.
     */
    public List<String> execute() {
        final Map<String, List<String>> mapLinesPerTable = listToMap(readFile(path, LAYOUT_FILE_BASE_NAME),
                String::isEmpty);

        return mapLinesPerTable.entrySet().stream().filter(Objects::nonNull)
                .flatMap(entry -> process(entry).stream()).collect(toList());
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
        if (!entry.getValue().stream().sorted().collect(joining(SEPARATOR))
                .equals(readFile(path, String.format("%s_%s", entry.getKey(), LAYOUT_FILE_BASE_NAME)).stream().sorted()
                        .collect(joining(SEPARATOR)))) {
            throw new RuntimeException(entry.getKey());
        }
    }

    /**
     * Receives a table's layout data, gets its data and generates inserts to this table with the data.
     *
     * @param entry Map.Entry with the table's name as key and List of table's layout information as value.
     * @return List of String containing a SQL insert generated by the code.
     */
    private List<String> process(Map.Entry<String, List<String>> entry) {
        validate(entry);

        final List<Map<String, String>> processedDataList = synchronizedList(new ArrayList<>());

        final List<String> layout = synchronizedList(entry.getValue());
        // Remove header line from layout
        layout.remove(LAYOUT_HEADER);

        final List<String> fileData = readFile(path, String.format("%s%s", entry.getKey(), TXT_EXTENSION));
        final List<String> columns = synchronizedList(
                layout.stream().map(line -> line.split(SEPARATOR)[0]).sorted().collect(toList()));

        fileData.parallelStream().forEach(fileLine -> {
            final Map<String, String> data = synchronizedMap(new HashMap<>());
            layout.parallelStream().forEach(layoutLine -> {
                final String[] layoutInfo = layoutLine.split(SEPARATOR);
                // layoutInfo[0] = Column name
                // layoutInfo[2] = Start position of information
                // layoutInfo[3] = End position of information
                // Next line maps the column value to its content
                data.put(layoutInfo[0], fileLine.substring(Integer.parseInt(layoutInfo[2]),
                        Integer.parseInt(layoutInfo[3])));
            });
            processedDataList.add(data);
        });

        return processedDataList.parallelStream().filter(Objects::nonNull)
                .map(processedData -> generateInsert(processedData, entry.getKey(), columns)).collect(toList());
    }

    /**
     * Generates a SQL insert string based on parameters.
     *
     * @param processedData Map with a column name as key and its data as value.
     * @param tableName     The table name.
     * @param columnList    List of columns to the insert.
     * @return String with the SQL insert generated.
     */
    private String generateInsert(Map<String, String> processedData, String tableName,
                                  List<String> columnList) {
        final StringBuffer insert = new StringBuffer();
        insert.append("INSERT INTO ").append(tableName).append(" (");
        insert.append(columnList.stream().collect(joining(SEPARATOR))).append(") VALUES (");
        insert.append(
                columnList.stream().map(processedData::get).map(Optional::ofNullable).map(value -> value.orElse(NULL))
                        .map(value -> (!value.trim().isEmpty()) ? value : NULL).collect(joining(SEPARATOR)))
                .append(");");
        return insert.toString();
    }
}
