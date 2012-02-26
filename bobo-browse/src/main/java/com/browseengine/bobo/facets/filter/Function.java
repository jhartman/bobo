package com.browseengine.bobo.facets.filter;

public interface Function<T, V> {
    public V apply(T arg);
}
