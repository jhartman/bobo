package com.browseengine.bobo.facets.range;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;
import com.browseengine.bobo.facets.filter.Function;

public class MultiDataCacheBuilder implements Function<BoboIndexReader, MultiValueFacetDataCache<?>> {
    public MultiDataCacheBuilder(String name) {
        this.name = name;
    }

    private final String name;

    @Override
    public MultiValueFacetDataCache<?> apply(BoboIndexReader reader) {
        return (MultiValueFacetDataCache) reader.getFacetData(name);
    }
}
