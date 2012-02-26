package com.browseengine.bobo.facets.filter;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.docidset.EmptyDocIdSet;
import com.browseengine.bobo.docidset.RandomAccessDocIdSet;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.util.Tuple2;
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

public class GenericAdaptiveFacetFilter extends RandomAccessFilter {
    private static final long serialVersionUID = 1L;
    private static Logger logger = Logger.getLogger(GenericAdaptiveFacetFilter.class);

    private final String facetName;
    private final Function<BoboIndexReader, FacetDataCache<?>> cacheBuilder;
    private final Function<FacetDataCache<?>, List<String>> buildTerms;
    private final Function<Tuple2<FacetDataCache<?>, List<String>>, Integer> calcHits;

    private final RandomAccessFilter innerFilter;

    private final boolean invertSelectivity;

    protected GenericAdaptiveFacetFilter(String facetName,
                                         Function<BoboIndexReader, FacetDataCache<?>> cacheBuilder,
                                         Function<FacetDataCache<?>, List<String>> buildTerms,
                                         Function<Tuple2<FacetDataCache<?>, List<String>>, Integer> calcHits,
                                         RandomAccessFilter innerFilter,
                                         boolean useComplement) {
        this.facetName = facetName;
        this.cacheBuilder = cacheBuilder;
        this.buildTerms = buildTerms;
        this.innerFilter = innerFilter;
        this.invertSelectivity = useComplement;
        this.calcHits = calcHits;
    }

    protected GenericAdaptiveFacetFilter(String facetName,
                                         Function<BoboIndexReader, FacetDataCache<?>> cacheBuilder,
                                         Function<FacetDataCache<?>, List<String>> buildTerms,
                                         RandomAccessFilter innerFilter,
                                         boolean useComplement) {
        this(facetName, cacheBuilder, buildTerms, new Function<Tuple2<FacetDataCache<?>, List<String>>, Integer>() {
            @Override
            public Integer apply(Tuple2<FacetDataCache<?>, List<String>> arguments) {
                FacetDataCache<?> facetDataCache = arguments.getFirst();
                List<String> terms = arguments.getSecond();
                int freqCount = 0;
                for(String term : terms) {
                    int idx = facetDataCache.valArray.indexOf(term);
                    freqCount += facetDataCache.freqs[idx];
                }
                return freqCount; 
            }
        }, innerFilter, useComplement);
    }


    public double getFacetSelectivity(BoboIndexReader reader) {
        double selectivity = innerFilter.getFacetSelectivity(reader);
        if (invertSelectivity)
            return 1.0 - selectivity;
        return selectivity;
    }

    private static final double LOG2 = Math.log(2);
    private static final int INVERTED_INDEX_PENALTY = 75;

    @Override
    public RandomAccessDocIdSet getRandomAccessDocIdSet(BoboIndexReader reader)
            throws IOException {

        RandomAccessDocIdSet innerDocSet = innerFilter.getRandomAccessDocIdSet(reader);
        if (innerDocSet == EmptyDocIdSet.getInstance()) {
            return innerDocSet;
        }

        FacetDataCache<?> facetDataCache = this.cacheBuilder.apply(reader);

        int totalCount = reader.maxDoc();
        
        List<String> terms = buildTerms.apply(facetDataCache);

        int freqCount = calcHits.apply(Tuple2.<FacetDataCache<?>, List<String>>of(facetDataCache, terms));
        
        if (terms.size() == 0) {
            return EmptyDocIdSet.getInstance();
        }

        int validFreqCount = invertSelectivity ? (totalCount - freqCount) : freqCount;

        double invertedIndexCost = INVERTED_INDEX_PENALTY * (Math.log(terms.size()) / LOG2) * validFreqCount;
        double forwardIndexCost = totalCount;

        if (facetName != null && (invertedIndexCost < forwardIndexCost)) {
            return new TermListRandomAccessDocIdSet(facetName, innerDocSet, terms, reader);
        } else {
            return innerDocSet;
        }
    }

    public static class TermListRandomAccessDocIdSet extends RandomAccessDocIdSet {

        private final RandomAccessDocIdSet _innerSet;
        private final List<String> _terms;
        private final IndexReader _reader;
        private final String _name;

        TermListRandomAccessDocIdSet(String name, RandomAccessDocIdSet innerSet, List<String> vals, IndexReader reader) {
            _name = name;
            _innerSet = innerSet;
            _terms = vals;
            _reader = reader;
        }

        public static class TermDocIdSet extends DocIdSet {
            final Term term;
            private final IndexReader reader;

            public TermDocIdSet(IndexReader reader, String name, String val) {
                this.reader = reader;
                term = new Term(name, val);
            }

            @Override
            public DocIdSetIterator iterator() throws IOException {
                final TermDocs td = reader.termDocs(term);
                if (td == null) {
                    return EmptyDocIdSet.getInstance().iterator();
                }
                return new DocIdSetIterator() {

                    private int _doc = -1;

                    @Override
                    public int advance(int target) throws IOException {
                        if (td.skipTo(target)) {
                            _doc = td.doc();
                        } else {
                            td.close();
                            _doc = DocIdSetIterator.NO_MORE_DOCS;
                        }
                        return _doc;
                    }

                    @Override
                    public int docID() {
                        return _doc;
                    }

                    @Override
                    public int nextDoc() throws IOException {
                        if (td.next()) {
                            _doc = td.doc();
                        } else {
                            td.close();
                            _doc = DocIdSetIterator.NO_MORE_DOCS;
                        }
                        return _doc;
                    }

                };
            }
        }

        @Override
        public boolean get(int docId) {
            return _innerSet.get(docId);
        }

        @Override
        public DocIdSetIterator iterator() throws IOException {
            if (_terms.size() == 0) {
                return EmptyDocIdSet.getInstance().iterator();
            }
            if (_terms.size() == 1) {
                return new TermDocIdSet(_reader, _name, _terms.get(0)).iterator();
            } else {
                List<DocIdSet> docSetList = new ArrayList<DocIdSet>(_terms.size());
                for (String term : _terms) {
                    docSetList.add(new TermDocIdSet(_reader, _name, term));
                }
                return new OrDocIdSet(docSetList).iterator();
            }
        }
    }
}
