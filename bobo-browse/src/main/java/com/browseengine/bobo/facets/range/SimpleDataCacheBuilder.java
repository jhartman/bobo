package com.browseengine.bobo.facets.range;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.filter.Function;

public class SimpleDataCacheBuilder implements Function<BoboIndexReader, FacetDataCache<?>> {
    public SimpleDataCacheBuilder(String name) {
        this.name = name;
    }

    private final String name;

    @Override
    public FacetDataCache<?> apply(BoboIndexReader arg) {
        return (FacetDataCache<?>) arg.getFacetData(name);
    }
}
