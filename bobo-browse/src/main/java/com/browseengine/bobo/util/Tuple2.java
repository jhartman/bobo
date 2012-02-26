package com.browseengine.bobo.util;

public final class Tuple2<T, V> {
    private final T first;
    private final V second;

    private Tuple2(T first, V second) {
        this.first = first;
        this.second = second;
    }
    
    public static <T, V> Tuple2<T, V> of(T first, V second) {
        return new Tuple2<T, V>(first, second);
    }

    public T getFirst() {
        return first;
    }

    public V getSecond() {
        return second;
    }
}
