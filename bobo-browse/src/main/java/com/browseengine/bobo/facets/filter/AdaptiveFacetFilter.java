package com.browseengine.bobo.facets.filter;

import java.io.IOException;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.docidset.EmptyDocIdSet;
import com.browseengine.bobo.docidset.RandomAccessDocIdSet;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.TermValueList;
import com.kamikaze.docidset.impl.OrDocIdSet;

public class AdaptiveFacetFilter extends GenericAdaptiveFacetFilter {
    public AdaptiveFacetFilter(String facetName, 
                               Function<BoboIndexReader, FacetDataCache<?>> cacheBuilder, 
                               RandomAccessFilter innerFilter,
                               final List<String> terms,
                               boolean useComplement) {
        super(facetName, cacheBuilder, new Function<FacetDataCache<?>, List<String>>() {
            @Override
            public List<String> apply(FacetDataCache<?> arg) {
                return terms;
            }
        }, innerFilter, useComplement);
    }
}
