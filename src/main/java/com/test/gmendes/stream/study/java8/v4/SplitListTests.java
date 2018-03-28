package com.test.gmendes.stream.study.java8.v4;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * This class contains some test implementations for listToMap method, for study. Chosen implementation is not listed
 * here.
 *
 * @author grmendes
 */
public class SplitListTests {

    /**
     * Converts the received stream into a Stream of lists, splitting it by the predicate informed.
     * <p>
     * Using LinkedList implementation over ArrayList.
     *
     * @param list List to be converted.
     * @param sep  Predicate indicating the separator in use.
     * @param <T>  Type used by the input list, the key and the list's elements of the output map and the predicate's
     *             evaluation.
     * @return Stream with a List of T elements.
     */
    public static <T> Stream<List<T>> splitListLinkedList(Stream<T> list, Predicate<T> sep) {
        return list.reduce(new LinkedList<List<T>>(),
                (l, elem) -> {
                    if (l.isEmpty()) {
                        l.add(new LinkedList<>());
                    }
                    if (sep.test(elem)) {
                        l.add(new LinkedList<>());
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
     * Converts the received stream into a Stream of lists, splitting it by the predicate informed.
     * <p>
     * Trying not to use a terminal operation (such as reduce), replacing it with some ThreadLocal variables, used to
     * cache data for Stream processing.
     *
     * @param list List to be converted.
     * @param sep  Predicate indicating the separator in use.
     * @param <T>  Type used by the input list, the key and the list's elements of the output map and the predicate's
     *             evaluation.
     * @return Stream with a List of T elements.
     */
    public static <T> Stream<List<T>> splitListThreadLocal(final Stream<T> list, final Predicate<T> sep) {
        ThreadLocal<Boolean> first = ThreadLocal.withInitial(() -> true);
        ThreadLocal<List<T>> group = ThreadLocal.withInitial(LinkedList::new);
        return list
                .map(elem -> {
                    List<T> ret = null;
                    if (first.get()) {
                        ret = group.get();
                        first.set(false);
                    }
                    if (sep.test(elem)) {
                        group.remove();
                        first.set(true);
                    } else {
                        group.get().add(elem);
                    }
                    return ret;
                }).filter(Objects::nonNull);
    }

    /**
     * Converts the received stream into a Stream of lists, splitting it by the predicate informed.
     * <p>
     * Trying not to use a terminal operation (such as reduce), replacing it with some Atomic variables, used to
     * cache data for Stream processing.
     *
     * @param list List to be converted.
     * @param sep  Predicate indicating the separator in use.
     * @param <T>  Type used by the input list, the key and the list's elements of the output map and the predicate's
     *             evaluation.
     * @return Stream with a List of T elements.
     */
    public static <T> Stream<List<T>> splitListAtomic(final Stream<T> list, final Predicate<T> sep) {
        AtomicBoolean first = new AtomicBoolean(true);
        AtomicReference<List<T>> group = new AtomicReference<>(new LinkedList<>());
        return list
                .map(elem -> {
                    List<T> ret = null;
                    if (first.get()) {
                        ret = group.get();
                        first.set(false);
                    }
                    if (sep.test(elem)) {
                        group.set(new LinkedList<>());
                        first.set(true);
                    } else {
                        group.get().add(elem);
                    }
                    return ret;
                }).filter(Objects::nonNull);
    }

    /**
     * Converts the received stream into a Stream of lists, splitting it by the predicate informed.
     * <p>
     * Trying not to use a terminal operation (such as reduce), replacing it with some Array variables, used to
     * cache data for Stream processing.
     *
     * @param list List to be converted.
     * @param sep  Predicate indicating the separator in use.
     * @param <T>  Type used by the input list, the key and the list's elements of the output map and the predicate's
     *             evaluation.
     * @return Stream with a List of T elements.
     */
    @SuppressWarnings("unchecked")
    public static <T> Stream<List<T>> splitListArray(final Stream<T> list, final Predicate<T> sep) {
        Boolean[] first = new Boolean[]{true};
        List<T>[] group = new List[]{new LinkedList<>()};
        return list
                .map(elem -> {
                    List<T> ret = null;
                    if (first[0]) {
                        ret = group[0];
                        first[0] = false;
                    }
                    if (sep.test(elem)) {
                        group[0] = new LinkedList<>();
                        first[0] = true;
                    } else {
                        group[0].add(elem);
                    }
                    return ret;
                }).filter(Objects::nonNull);
    }
}
