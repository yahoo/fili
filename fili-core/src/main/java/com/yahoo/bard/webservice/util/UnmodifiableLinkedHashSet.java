package com.yahoo.bard.webservice.util;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class UnmodifiableLinkedHashSet<E> extends LinkedHashSet<E> {

    private final Set<E> target;

    private UnmodifiableLinkedHashSet(Set<E> target) {
        this.target = Collections.unmodifiableSet(new LinkedHashSet<>(Objects.requireNonNull(target)));
    }

    public static <E> UnmodifiableLinkedHashSet<E> of(LinkedHashSet<E> target) {
        return new UnmodifiableLinkedHashSet<>(target);
    }

    @Override public boolean equals(Object o) {
        return target.equals(o);
    }
    @Override public int hashCode() {
        return target.hashCode();
    }
    @Override public String toString() {
        return target.toString();
    }

    @Override public int size() {
        return target.size();
    }
    @Override public boolean isEmpty() {
        return target.isEmpty();
    }
    @Override public boolean contains(Object o) {
        return target.contains(o);
    }
    @Override public Object[] toArray() {
        return target.toArray();
    }
    @Override public <T> T[] toArray(T[] a) {
        return target.toArray(a);
    }
    @Override public Iterator<E> iterator() {
        return target.iterator();
    }

    @Override public boolean add(E e) {
        return target.add(e);
    }
    @Override public boolean remove(Object o) {
        return target.remove(o);
    }

    @Override public boolean containsAll(Collection<?> coll) {
        return target.containsAll(coll);
    }
    @Override public boolean addAll(Collection<? extends E> coll) {
        return target.addAll(coll);
    }
    @Override public boolean removeAll(Collection<?> coll) {
        return target.removeAll(coll);
    }
    @Override public boolean retainAll(Collection<?> coll) {
        return target.retainAll(coll);
    }
    @Override public void clear() {
        target.clear();
    }

    @Override public void forEach(Consumer<? super E> action) {
        target.forEach(action);
    }
    @Override public boolean removeIf(Predicate<? super E> filter) {
        return target.removeIf(filter);
    }
    @SuppressWarnings("unchecked") @Override public Spliterator<E> spliterator() {
        return target.spliterator();
    }
    @SuppressWarnings("unchecked") @Override public Stream<E> stream() {
        return target.stream();
    }
    @SuppressWarnings("unchecked") @Override public Stream<E> parallelStream() {
        return target.parallelStream();
    }
}
