package com.test.gmendes.stream.study.java7.v1;

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
 * This class holds basically the Java 8's first version translated into Java 7. The same structure of code is used,
 * so the performance comparison is plenty reasonable. Please note that lots of codes in here can be (and already was)
 * improved, either for performance or readability.
 *
 * @author grmendes
 * @version 1
 */
public class ProcessJava7V1 {

    private final String path;

    public ProcessJava7V1(String path) {
        this.path = path;
    }

    /**
     * Reads all file lines.
     * <p>
     * Based on Java 6's default file reading code.
     *
     * @param path     Path to the file.
     * @param filename Name of the file.
     * @return List of String with all file's lines.
     */
    public static List<String> readFile(String path, String filename) {
        List<String> lines = new ArrayList<>();

        FileInputStream fileInputStream = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader bufferedReader = null;
        try {
            fileInputStream = new FileInputStream(new File(path, filename));
            inputStreamReader = new InputStreamReader(fileInputStream, ISO_8859_1);
            bufferedReader = new BufferedReader(inputStreamReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                lines.add(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (inputStreamReader != null) {
                try {
                    inputStreamReader.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        return lines;
    }

    /**
     * Converts the received list into a map, splitting it by empty lines. For each sublist generated, removes its
     * first element and uses it as the Map's key.
     * <p>
     * Dummy implementation, written with the easiest solution possible for this problem, iterating the list to compute
     * a splitted list and iterating the splitted list to compute the map for returning.
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
        List<List<String>> listOfList = new ArrayList<>();

        listOfList.add(new ArrayList<>());

        // First step: Splits the input list into a list of lists by empty elements on the list.
        for (String string : list) {
            if (string.isEmpty()) { // If reaches the separator, starts over a new list.
                listOfList.add(new ArrayList<>());
            } else { // If it's not the separator, adds the value to its list.
                listOfList.get(listOfList.size() - 1).add(string);
            }
        }

        // Second step: Construct a map based on the list of lists, using each sublist's first element as key for the
        // map.
        for (List<String> strings : listOfList) {
            if (!strings.isEmpty()) { // Gets the key of the list and puts the list with the key into the map.
                String key = strings.remove(0);
                map.put(key, strings);
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
        String layout = "";
        String file = "";

        sort(entry.getValue());

        for (String string : entry.getValue()) {
            layout += string;
            layout += SEPARATOR;
        }
        layout = layout.substring(0, layout.length() - 1);

        final List<String> fileLines = readFile(path,
                String.format("%s_%s", entry.getKey(), LAYOUT_FILE_BASE_NAME));
        sort(fileLines);

        for (String line : fileLines) {
            file += line;
            file += SEPARATOR;
        }
        file = file.substring(0, file.length() - 1);

        if (!layout.equals(file)) {
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

        final List<Map<String, String>> processedDataList = new ArrayList<>();

        final List<String> layout = entry.getValue();
        // Remove header line from layout
        layout.remove(LAYOUT_HEADER);

        final List<String> fileData = readFile(path, String.format("%s%s", entry.getKey(), TXT_EXTENSION));

        final List<String> columns = new ArrayList<>();
        for (String line : layout) {
            columns.add(line.split(SEPARATOR)[0]);
        }

        sort(columns);

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
            processedDataList.add(data);
        }

        final List<String> inserts = new ArrayList<>();
        for (Map<String, String> processedData : processedDataList) {
            inserts.add(generateInsert(processedData, entry.getKey(), columns));
        }
        return inserts;
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
        final StringBuilder insert = new StringBuilder();
        insert.append("INSERT INTO ").append(tableName).append(" (");
        String columns = "";

        for (String column : columnList) {
            columns += column;
            columns += SEPARATOR;
        }

        columns = columns.substring(0, columns.length() - 1);

        insert.append(columns).append(") VALUES (");

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
