package com.test.gmendes.stream.study.java7.v2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.test.gmendes.stream.study.Constants.LAYOUT_FILE_BASE_NAME;
import static com.test.gmendes.stream.study.Constants.LAYOUT_HEADER;
import static com.test.gmendes.stream.study.Constants.NULL;
import static com.test.gmendes.stream.study.Constants.SEPARATOR;
import static com.test.gmendes.stream.study.Constants.TXT_EXTENSION;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.util.Collections.sort;

/**
 * This class contains the Java 7 based implementation of the project. The main goal is to process a batch of files,
 * generating SQL insert strings, as proof of concept.
 * <p>
 * The main goal is to not use any Java 8 new features into this implementation.
 * <p>
 * This class holds basically the Java 8's second version translated into Java 7. The same structure of code is used,
 * so the performance comparison is plenty reasonable. Please note that some optimization was implemented in a different
 * approach, comparing to Java 8's second version.
 *
 * @author grmendes
 * @version 2
 */
public class ProcessJava7V2 {

    private final String path;

    public ProcessJava7V2(String path) {
        this.path = path;
    }

    /**
     * Reads all file lines.
     * <p>
     * Based on Java 7's try with resources default file reading code.
     *
     * @param path     Path to the file.
     * @param filename Name of the file.
     * @return List of String with all file's lines.
     */
    public static List<String> readFile(String path, String filename) {
        List<String> lines = new ArrayList<>();

        try (FileInputStream fileInputStream = new FileInputStream(new File(path, filename))) {
            try (InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, ISO_8859_1)) {
                try (BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
                    String line;
                    while ((line = bufferedReader.readLine()) != null) {
                        lines.add(line);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return lines;
    }

    /**
     * Converts the received list into a map, splitting it by empty lines. For each sublist generated, removes its
     * first element and uses it as the Map's key.
     * <p>
     * Best approach implementation. Iterates once the input list to compute the output map directly, with no need to
     * post processing anything.
     * <p>
     * Example:
     * <li>input list = ["Header", "1", "2", "", "Header2", "3"]</li>
     * <li>output map = [{"Header", ["1", "2"]}, {"Header2", ["3"]}]</li>
     *
     * @param list List to be converted.
     * @return Map with a String key and a List of Strings value.
     */
    public static Map<String, List<String>> listToMap(List<String> list) {
        Map<String, List<String>> map = new HashMap<>();
        boolean newList = true;
        String key = null;

        // Construct a map based on the input list. No splitting step needed with this approach.
        for (String line : list) {
            if (line.isEmpty()) { // If reaches the separator, starts over a new list.
                newList = true;
            } else if (newList) { // If it's not the separator and is a new list, gets the key of this list and starts over the map.
                key = line;
                newList = false;
                map.put(key, new ArrayList<>());
            } else { // If it's not the separator and it's not a new list, uses current key and adds the value to its list.
                map.get(key).add(line);
            }
        }

        return map;
    }

    /**
     * Entry point to start processing for testing. Generates a list of SQL inserts, based on the files processed.
     *
     * @return List of the generated inserts.
     */
    public List<String> execute() {

        final Map<String, List<String>> mapLinesPerTable = listToMap(readFile(path, LAYOUT_FILE_BASE_NAME));

        final List<String> inserts = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : mapLinesPerTable.entrySet()) {
            inserts.addAll(process(entry));
        }
        return inserts;
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
        String layout = groupListToString(entry.getValue());

        String file = groupListToString(readFile(path,
                entry.getKey() + "_" + LAYOUT_FILE_BASE_NAME));

        if (!layout.equals(file)) {
            throw new RuntimeException(entry.getKey());
        }
    }

    /**
     * Groups all List members into one String, using a default separator.
     *
     * @param list List to be grouped into String.
     * @return String containing all List's members separated by default SEPARATOR.
     */
    private String groupListToString(List<String> list) {
        String text = "";
        sort(list);

        for (String line : list) {
            text += line;
            text += SEPARATOR;
        }
        return text.substring(0, text.length() - 1);
    }

    /**
     * Receives a table's layout data, gets its data and generates inserts to this table with the data.
     *
     * @param entry Map.Entry with the table's name as key and List of table's layout information as value.
     * @return List of String containing a SQL insert generated by the code.
     */
    private List<String> process(Map.Entry<String, List<String>> entry) {
        validate(entry);

        final List<String> layout = entry.getValue();
        // Remove header line from layout
        layout.remove(LAYOUT_HEADER);

        String tableName = entry.getKey();
        final List<String> fileData = readFile(path, tableName + TXT_EXTENSION);

        final List<String> columns = new ArrayList<>();
        for (String line : layout) {
            columns.add(line.split(SEPARATOR)[0]);
        }

        String columnsString = groupListToString(columns);

        final String baseInsertText = "INSERT INTO " + tableName + " (" + columnsString + ") VALUES (";

        final List<String> inserts = new ArrayList<>();

        for (String fileLine : fileData) {
            final Map<String, String> data = new HashMap<>();
            for (String layoutLine : layout) {
                final String[] layoutInfo = layoutLine.split(SEPARATOR);
                // layoutInfo[0] = Column name
                // layoutInfo[2] = Start position of information
                // layoutInfo[3] = End position of information
                // Next line maps the column value to its content
                data.put(layoutInfo[0], fileLine.substring(Integer.parseInt(layoutInfo[2]),
                        Integer.parseInt(layoutInfo[3])));
            }
            inserts.add(generateInsert(data, baseInsertText, columns));
        }

        return inserts;
    }

    /**
     * Generates a SQL insert string based on parameters.
     *
     * @param processedData  Map with a column name as key and its data as value.
     * @param baseInsertText The base insert text, which contains all information before the values itself.
     * @param columnList     List of columns to the insert.
     * @return String with the SQL insert generated.
     */
    private String generateInsert(Map<String, String> processedData, String baseInsertText,
                                  List<String> columnList) {
        final StringBuilder insert = new StringBuilder(baseInsertText);

        String data = "";

        for (String column : columnList) {
            String value = processedData.get(column);
            if (value == null || value.trim().isEmpty()) {
                value = NULL;
            }
            data += value;
            data += SEPARATOR;
        }

        data = data.substring(0, data.length() - 1);

        insert.append(data).append(");");
        return insert.toString();
    }
}
