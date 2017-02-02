package com.test.gmendes.stream.study.java7.v2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class contains a test implementation for listToMap method, for study. Chosen implementation is not listed
 * here.
 *
 * @author grmendes
 */
public class ListToMapJava7Tests {

    private ListToMapJava7Tests() {
        // Private Default Constructor.
    }

    /**
     * Converts the received list into a map, splitting it by the predicate informed. For each sublist generated,
     * removes its first element and uses it as the Map's key.
     * <p>
     * Iterates the list and identifies all positions where the predicate's test is valid. Collect those positions to an
     * array and then generates sublists of the input list based on the positions.
     * <p>
     * Based on <a href='http://stackoverflow.com/questions/29095967/splitting-list-into-sublists-along-elements'>this Stackoverflow</a>.
     * <p>
     * Example:
     * <li>input list = ["Header", "1", "2", "", "Header2", "3"]</li>
     * <li>input predicate = String::isEmpty</li>
     * <li>output map = [{"Header", ["1", "2"]}, {"Header2", ["3"]}]</li>
     *
     * @param list List to be converted.
     * @return Map with a String key and a List of String as value.
     */
    public static Map<String, List<String>> listToMap7Edges(List<String> list) {
        List<List<String>> listOfLists = new ArrayList<>();
        Map<String, List<String>> map = new HashMap<>();
        int start = 0;

        for (int cur = 0; cur < list.size(); cur++) {
            if (list.get(cur).isEmpty()) {
                listOfLists.add(new ArrayList<>(list.subList(start, cur)));
                start = cur + 1;
            }
        }
        listOfLists.add(new ArrayList<>(list.subList(start, list.size())));

        for (List<String> sublist : listOfLists) {
            if (!sublist.isEmpty()) {
                map.put(sublist.remove(0), sublist);
            }
        }

        return map;
    }
}
