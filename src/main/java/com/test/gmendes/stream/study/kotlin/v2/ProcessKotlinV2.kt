package com.test.gmendes.stream.study.kotlin.v2

import com.test.gmendes.stream.study.Constants.LAYOUT_FILE_BASE_NAME
import com.test.gmendes.stream.study.Constants.LAYOUT_HEADER
import com.test.gmendes.stream.study.Constants.NULL
import com.test.gmendes.stream.study.Constants.SEPARATOR
import com.test.gmendes.stream.study.Constants.TXT_EXTENSION
import java.io.File
import java.nio.charset.StandardCharsets.ISO_8859_1
import java.util.*

/**
 * This class contains the Kotlin based implementation of the project. The main goal is to process a batch of files,
 * generating SQL insert strings, as proof of concept.
 *
 * The main idea is to learn how to code using Kotlin into this implementation.
 *
 * This class holds basically the Java 8's fourth version ideas translated into Kotlin. The same structure of code is used,
 * so the performance comparison is plenty reasonable. Please note that some optimization was implemented in a different
 * approach, comparing to Java 8's fourth version.
 *
 * @author grmendes
 *
 * @version 2
 */
class ProcessKotlinV2(private val path: String) {

    /**
     * Entry point to start processing for testing. Generates a list of SQL inserts, based on the files processed.

     * @return List of the generated inserts.
     */
    fun execute(): List<String> {
        return splitList(readFile(path, LAYOUT_FILE_BASE_NAME)).flatMap { process(it).toList() }
    }

    /**
     * Validates the quality of input. This step is not strictly needed, but simulates a real scenario.
     * The data to process contains a file named layout.txt, which contains all tables, tables' columns and positional
     * information for data extraction. Also, there's a <TABLE_NAME>_layout.txt file with specific table columns
     * and positional information for data extraction. Both general layout and table specific layout files must contain
     * the same information. This method validates if both information are the same.
     *
     * This method receives a list with the information from the general layout file (layoutList param) and
     * reads a list containing the information from the table's specific layout file. Then, it compares both with
     * List.containsAll method.
     *
     * @param tableName  the table name.
     * @param layoutList the general layout's content.
     * @throws RuntimeException if results are not the same. Not supposed to occur.
     */
    private fun validate(tableName: String, layoutList: List<String>) {
        when { readFile(path, tableName + "_" + LAYOUT_FILE_BASE_NAME).toList() != layoutList -> throw RuntimeException(
                tableName)
        }
    }

    /**
     * Receives a table's layout data, gets its data and generates inserts to this table with the data.
     *
     * @param layoutList MutableList with the table's name as first element and List of table's layout information as
     * other elements.
     * @return Sequence of String containing a SQL insert generated by the code.
     */
    private fun process(layoutList: MutableList<String>): Sequence<String> {
        val tableName = layoutList.removeAt(0)
        validate(tableName, layoutList)

        layoutList.remove(LAYOUT_HEADER)

        val columns = layoutList.map { layoutLine -> layoutLine.split(SEPARATOR.toRegex())[0] }.sorted()

        val baseInsertText = columns.joinToString(SEPARATOR, "INSERT INTO $tableName (", ") VALUES (")

        val layoutInfo = layoutList.map { it.split(SEPARATOR.toRegex()).dropLastWhile { it.isEmpty() } }
        return readFile(path, tableName + TXT_EXTENSION).map { fileLine ->
            layoutInfo.map { it[0] to fileLine.substring(it[2].toInt(), it[3].toInt()) }.toMap()
        }.map {
            generateInsert(it, baseInsertText, columns)
        }
    }

    /**
     * Generates a SQL insert string based on parameters.
     *
     * @param processedData  Map with a column name as key and its data as value.
     * @param baseInsertText The base insert text, which contains all information before the values itself.
     * @param columnList     List of columns to the insert.
     * @return String with the SQL insert generated.
     */
    private fun generateInsert(processedData: Map<String, String>, baseInsertText: String,
                               columnList: List<String>): String {
        return "$baseInsertText${columnList.map { processedData[it] }.joinToString(SEPARATOR) {
            if (it == null || it.trim { it <= ' ' }.isEmpty()) NULL else it
        }});"
    }

    companion object {

        /**
         * Reads all file lines.
         *
         * Based on Kotlin default file reading code.
         *
         * @param path     Path to the file.
         * @param filename Name of the file.
         * @return Sequence of String with all file's lines.
         */
        fun readFile(path: String, filename: String): Sequence<String> {
            val list = ArrayList<String>()
            File(path, filename).useLines(ISO_8859_1, { it.forEach { list.add(it) } })
            return list.asSequence()
        }

        /**
         * Converts the received Sequence into a List of lists, splitting it by the predicate informed.
         *
         * @param list Sequence to be converted.
         * @return List with a List of String elements.
         */
        fun splitList(list: Sequence<String>): List<ArrayList<String>> {
            return list.fold<String, ArrayList<ArrayList<String>>>(ArrayList(),
                    { array, string ->
                        when {
                            array.isEmpty() -> array.add(ArrayList())
                        }
                        when {
                            string.isEmpty() -> array.add(ArrayList())
                            else -> array.last().add(string)
                        }
                        array
                    }).filter { it.isNotEmpty() }
        }
    }
}
