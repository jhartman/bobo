package com.browseengine.bobo.facets.filter;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.docidset.EmptyDocIdSet;
import com.browseengine.bobo.docidset.RandomAccessDocIdSet;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.TermValueList;
import com.kamikaze.docidset.impl.OrDocIdSet;
import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AdaptiveRangeFacetFilter extends GenericAdaptiveFacetFilter {

    public AdaptiveRangeFacetFilter(String facetName, 
                                    Function<BoboIndexReader, FacetDataCache<?>> cacheBuilder, 
                                    RandomAccessFilter innerFilter,
                                    final String rangeString,
                                    boolean useComplement) {
        super(facetName, cacheBuilder, new Function<FacetDataCache<?>, List<String>>() {
            @Override
            public List<String> apply(FacetDataCache<?> facetDataCache) {
                int[] segments = FacetRangeFilter.parse(facetDataCache, rangeString);
                List<String> terms = new ArrayList<String>(segments[1] - segments[0] + 1);
                for(int docId = segments[0]; docId <= segments[1]; docId++) {
                    String term = facetDataCache.valArray.get(docId);
                    terms.add(term);
                }
                return terms;
            }
        }, innerFilter, useComplement);    
    }
}
