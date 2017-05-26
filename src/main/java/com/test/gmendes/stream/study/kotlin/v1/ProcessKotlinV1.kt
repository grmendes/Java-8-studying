package com.test.gmendes.stream.study.kotlin.v1

import java.util.ArrayList
import java.util.HashMap

import com.test.gmendes.stream.study.Constants.LAYOUT_FILE_BASE_NAME
import com.test.gmendes.stream.study.Constants.LAYOUT_HEADER
import com.test.gmendes.stream.study.Constants.NULL
import com.test.gmendes.stream.study.Constants.SEPARATOR
import com.test.gmendes.stream.study.Constants.TXT_EXTENSION
import java.nio.charset.StandardCharsets.ISO_8859_1
import java.nio.file.Files
import java.nio.file.Paths
import java.util.Collections.sort

/**
 * This class contains the Java 7 based implementation of the project. The main goal is to process a batch of files,
 * generating SQL insert strings, as proof of concept.
 *
 *
 * The main goal is to not use any Java 8 new features into this implementation.
 *
 *
 * This class holds basically the Java 8's second version translated into Java 7. The same structure of code is used,
 * so the performance comparison is plenty reasonable. Please note that some optimization was implemented in a different
 * approach, comparing to Java 8's second version.

 * @author grmendes
 * *
 * @version 1
 */
class ProcessKotlinV1(private val path: String) {

    /**
     * Entry point to start processing for testing. Generates a list of SQL inserts, based on the files processed.

     * @return List of the generated inserts.
     */
    fun execute(): List<String> {

        val mapLinesPerTable = listToMap(readFile(path, LAYOUT_FILE_BASE_NAME))

        val inserts = ArrayList<String>()
        for (entry in mapLinesPerTable.entries) {
            inserts.addAll(process(entry))
        }
        return inserts
    }

    /**
     * Validates the quality of input. This step is not strictly needed, but simulates a real scenario.
     * The data to process contains a file named layout.txt, which contains all tables, tables' columns and positional
     * information for data extraction. Also, there's a <TABLE_NAME>_layout.txt file with specific table columns
     * and positional information for data extraction. Both general layout and table specific layout files must contain
     * the same information. This method validates if both information are the same.
     *
     *
     * This method generates two ordered strings, one containing the information from the general layout file (entry
     * param) and the other containing the information from the table's specific layout file.

     * @param entry Map.Entry containing the table name as key and the general layout's content as value.
     * *
     * @throws RuntimeException if results are not the same. Not supposed to occur.
    </TABLE_NAME> */
    private fun validate(entry: Map.Entry<String, List<String>>) {
        val layout = groupListToString(entry.value)

        val file = groupListToString(readFile(path,
                entry.key + "_" + LAYOUT_FILE_BASE_NAME))

        if (layout != file) {
            throw RuntimeException(entry.key)
        }
    }

    /**
     * Groups all List members into one String, using a default separator.

     * @param list List to be grouped into String.
     * *
     * @return String containing all List's members separated by default SEPARATOR.
     */
    private fun groupListToString(list: List<String>): String {
        var text = ""
        sort<String>(list)

        for (line in list) {
            text += line
            text += SEPARATOR
        }
        return text.substring(0, text.length - 1)
    }

    /**
     * Receives a table's layout data, gets its data and generates inserts to this table with the data.

     * @param entry Map.Entry with the table's name as key and List of table's layout information as value.
     * *
     * @return List of String containing a SQL insert generated by the code.
     */
    private fun process(entry: Map.Entry<String, MutableList<String>>): List<String> {
        validate(entry)

        val layout = entry.value
        // Remove header line from layout
        layout.remove(LAYOUT_HEADER)

        val tableName = entry.key
        val fileData = readFile(path, tableName + TXT_EXTENSION)

        val columns = ArrayList<String>()
        for (line in layout) {
            columns.add(line.split(SEPARATOR.toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()[0])
        }

        val columnsString = groupListToString(columns)

        val baseInsertText = "INSERT INTO $tableName ($columnsString) VALUES ("

        val inserts = ArrayList<String>()

        for (fileLine in fileData) {
            val data = HashMap<String, String>()
            for (layoutLine in layout) {
                val layoutInfo = layoutLine.split(SEPARATOR.toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()
                // layoutInfo[0] = Column name
                // layoutInfo[2] = Start position of information
                // layoutInfo[3] = End position of information
                // Next line maps the column value to its content
                data.put(layoutInfo[0], fileLine.substring(Integer.parseInt(layoutInfo[2]),
                        Integer.parseInt(layoutInfo[3])))
            }
            inserts.add(generateInsert(data, baseInsertText, columns))
        }

        return inserts
    }

    /**
     * Generates a SQL insert string based on parameters.

     * @param processedData  Map with a column name as key and its data as value.
     * *
     * @param baseInsertText The base insert text, which contains all information before the values itself.
     * *
     * @param columnList     List of columns to the insert.
     * *
     * @return String with the SQL insert generated.
     */
    private fun generateInsert(processedData: Map<String, String>, baseInsertText: String,
                               columnList: List<String>): String {
        val insert = StringBuilder(baseInsertText)

        var data = ""

        for (column in columnList) {
            var value: String? = processedData[column]
            if (value == null || value.trim { it <= ' ' }.isEmpty()) {
                value = NULL
            }
            data += value
            data += SEPARATOR
        }

        data = data.substring(0, data.length - 1)

        insert.append(data).append(");")
        return insert.toString()
    }

    companion object {

        /**
         * Reads all file lines.
         *
         *
         * Based on Java 7's try with resources default file reading code.

         * @param path     Path to the file.
         * *
         * @param filename Name of the file.
         * *
         * @return List of String with all file's lines.
         */
        fun readFile(path: String, filename: String): List<String> {
            try {
                return Files.readAllLines(Paths.get(path, filename), ISO_8859_1)
            } catch (e: Exception) {
                e.printStackTrace()
            }

            return emptyList()
        }

        /**
         * Converts the received list into a map, splitting it by empty lines. For each sublist generated, removes its
         * first element and uses it as the Map's key.
         *
         *
         * Best approach implementation. Iterates once the input list to compute the output map directly, with no need to
         * post processing anything.
         *
         *
         * Example:
         *  * input list = ["Header", "1", "2", "", "Header2", "3"]
         *  * output map = [{"Header", ["1", "2"]}, {"Header2", ["3"]}]

         * @param list List to be converted.
         * *
         * @return Map with a String key and a List of Strings value.
         */
        fun listToMap(list: List<String>): Map<String, MutableList<String>> {
            val map = HashMap<String, MutableList<String>>()
            var newList = true
            var key: String? = null

            // Construct a map based on the input list. No splitting step needed with this approach.
            for (line in list) {
                if (line.isEmpty()) { // If reaches the separator, starts over a new list.
                    newList = true
                } else if (newList) { // If it's not the separator and is a new list, gets the key of this list and starts over the map.
                    key = line
                    newList = false
                    map.put(key, ArrayList<String>())
                } else { // If it's not the separator and it's not a new list, uses current key and adds the value to its list.
                    map[key]?.add(line)
                }
            }

            return map
        }
    }
}
