package com.test.gmendes.stream.study.java8.v2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toMap;

/**
 * This class contains some test implementations for listToMap method, for study. Chosen implementation is not listed
 * here.
 *
 * @author grmendes
 */
public class ListToMapJava8Tests {

    private ListToMapJava8Tests() {
        // Private Default Constructor.
    }

    private static boolean newList;
    private static Object key;

    /**
     * Converts the received list into a map, splitting it by empty lines. For each sublist generated, removes its
     * first element and uses it as the Map's key.
     * <p>
     * The same approach as ProcessJava7V2's, using lambdas. Iterates once the input list to compute the output map
     * directly, with no need to post processing anything.
     * It requires static variables to be declared for use inside the lambdas, which is not the safer approach.
     * <p>
     * Example:
     * <li>input list = ["Header", "1", "2", "", "Header2", "3"]</li>
     * <li>output map = [{"Header", ["1", "2"]}, {"Header2", ["3"]}]</li>
     *
     * @param list List to be converted.
     * @return Map with a String key and a List of Strings value.
     */
    @SuppressWarnings({"unchecked", "SuspiciousMethodCalls"})
    public static <T> Map<T, List<T>> listToMap8ForEach(List<T> list, Predicate<T> separator) {
        Map<T, List<T>> map = new HashMap<>();

        newList = true;
        list.forEach(t -> {
            if (separator.test(t)) { // If reaches the separator, starts over a new list.
                newList = true;
            } else if (newList) { // If it's not the separator and is a new list, gets the key of this list and starts over the map.
                key = t;
                newList = false;
                map.put((T) key, new ArrayList<>());
            } else { // If it's not the separator and it's not a new list, uses current key and adds the value to its list.
                map.get(key).add(t);
            }
        });

        return map;
    }

    /**
     * Collector to produce a splitted list according to predicate informed.
     * <p>
     * Simplified version, more readable.
     * <p>
     * Based on <a href='http://stackoverflow.com/questions/29095967/splitting-list-into-sublists-along-elements'>this Stackoverflow</a>.
     *
     * @param sep Predicate to indicate the separator to use.
     * @param <T> Type of the Stream's objects.
     * @return List of List of T, which is a list of splitted lists.
     */
    private static <T> Collector<T, List<List<T>>, List<List<T>>> splitBySeparator(Predicate<T> sep) {
        return Collector.of(ArrayList::new, // Supplies a new ArrayList
                (l, elem) -> {
                    if (l.isEmpty()) { // Adds a new element if the list is empty. Specially used on first interaction.
                        l.add(new ArrayList<>());
                    }
                    if (sep.test(elem)) { // If reaches the separator, starts over a new list.
                        l.add(new ArrayList<>());
                    } else { // If it's not the separator, adds the value to its list.
                        l.get(l.size() - 1).add(elem);
                    }
                },
                (l1, l2) -> { // Aggregates two lists
                    l1.addAll(l2);
                    return l1;
                });
    }

    /**
     * Converts the received list into a map, splitting it by the predicate informed. For each sublist generated,
     * removes its first element and uses it as the Map's key.
     * <p>
     * Uses less operations than ProcessJava8V1's implementation, but the approach is basically the same.
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
    public static <T> Map<T, List<T>> listToMap8CollectorOptimized(List<T> list, Predicate<T> sep) {
        return list.stream().collect(splitBySeparator(sep)).stream().filter(l -> !l.isEmpty())
                .collect(toMap(l -> l.remove(0), l -> l));
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
     * @param sep  Predicate indicating the separator in use.
     * @param <T>  Type used by the input list, the key and the list's elements of the output map and the predicate's
     *             evaluation.
     * @return Map with a T key and a List of T as value.
     */
    public static <T> Map<T, List<T>> listToMap8Edges(List<T> list, Predicate<T> sep) {
        int[] edges = IntStream.range(-1, list.size() + 1)
                .filter(i -> i == -1 || i == list.size() ||
                        sep.test(list.get(i)))
                .toArray();

        return IntStream.range(0, edges.length - 1)
                .mapToObj(k -> new ArrayList<>(list.subList(edges[k] + 1, edges[k + 1])))
                .filter(l -> !l.isEmpty())
                .collect(toMap(l -> l.remove(0), l -> l));
    }
}
