package com.browseengine.bobo.facets.data;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.FacetHandler.TermCountSize;
import com.browseengine.bobo.sort.DocComparator;
import com.browseengine.bobo.sort.DocComparatorSource;
import com.browseengine.bobo.util.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.OpenBitSet;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class FacetDataCache<T> implements Serializable {
    private static Logger logger = Logger.getLogger(FacetDataCache.class.getName());
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private final IntBuffer orderArray;
    protected final TermValueList<T> valArray;
    private final LazyBigIntArray freqs;
    private final IntBuffer minIDs;
    private final IntBuffer maxIDs;
    private final TermCountSize _termCountSize;
    private final int length;

    public FacetDataCache(int length, IntBuffer orderArray, TermValueList<T> valArray, LazyBigIntArray freqs, IntBuffer minIDs, IntBuffer maxIDs, TermCountSize _termCountSize) {
        this.orderArray = orderArray;
        this.valArray = valArray;
        this.freqs = freqs;
        this.minIDs = minIDs;
        this.maxIDs = maxIDs;
        this._termCountSize = _termCountSize;
        this.length = length;
    }

    public int getNumItems(int docid) {
        int valIdx = orderArray.get(docid);
        return valIdx <= 0 ? 0 : 1;
    }

//  private final static BigSegmentedArray newInstance(TermCountSize termCountSize, int maxDoc) {
//    if (termCountSize == TermCountSize.small) {
//      return new BigByteArray(maxDoc);
//    } else if (termCountSize == TermCountSize.medium) {
//      return new BigShortArray(maxDoc);
//    } else
//      return new BigIntArray(maxDoc);
//  }

    protected static int getNegativeValueCount(IndexReader reader, String field) throws IOException {
        int ret = 0;
        TermEnum termEnum = null;
        try {
            termEnum = reader.terms(new Term(field, ""));
            do {
                Term term = termEnum.term();
                if (term == null || term.field() != field)
                    break;
                if (!term.text().startsWith("-")) {
                    break;
                }
                ret++;
            } while (termEnum.next());
        } finally {
            termEnum.close();
        }
        return ret;
    }

    public static <T> FacetDataCache<T> load(String fieldName, IndexReader reader, TermListFactory<T> listFactory) throws IOException {
        String field = fieldName.intern();
        int maxDoc = reader.maxDoc();

//    IntBuffer orderArray = IntBuffer.allocate()
        IntBuffer orderArray = ByteBuffer.allocateDirect(4 * (1 + maxDoc)).asIntBuffer();

        IntArrayList minIDList = new IntArrayList();
        IntArrayList maxIDList = new IntArrayList();
        IntArrayList freqList = new IntArrayList();

        int length = maxDoc + 1;
        TermValueList<T> valArray = listFactory == null ? (TermValueList<T>) new TermStringList() : listFactory
                .createTermList();

        int negativeValueCount = getNegativeValueCount(reader, field);

        TermDocs termDocs = reader.termDocs();
        TermEnum termEnum = reader.terms(new Term(field, ""));
        int t = 0; // current term number

        valArray.add(null);
        minIDList.add(-1);
        maxIDList.add(-1);
        freqList.add(0);
        int totalFreq = 0;
        // int df = 0;
        t++;
        try {
            do {
                Term term = termEnum.term();
                if (term == null || term.field() != field)
                    break;

                if (t > Integer.MAX_VALUE) {
                    throw new IOException("maximum number of value cannot exceed: " + Integer.MAX_VALUE);
                }
                // store term text
                // we expect that there is at most one term per document
                if (t >= length)
                    throw new RuntimeException("there are more terms than " + "documents in field \"" + field
                            + "\", but it's impossible to sort on " + "tokenized fields");
                valArray.add(term.text());
                termDocs.seek(termEnum);
                // freqList.add(termEnum.docFreq()); // doesn't take into account
                // deldocs
                int minID = -1;
                int maxID = -1;
                int df = 0;
                int valId = (t - 1 < negativeValueCount) ? (negativeValueCount - t + 1) : t;
                if (termDocs.next()) {
                    df++;
                    int docid = termDocs.doc();

                    orderArray.put(docid, valId);
                    minID = docid;
                    while (termDocs.next()) {
                        df++;
                        docid = termDocs.doc();
                        orderArray.put(docid, valId);
                    }
                    maxID = docid;
                }
                freqList.add(df);
                totalFreq += df;
                minIDList.add(minID);
                maxIDList.add(maxID);

                t++;
            } while (termEnum.next());
        } finally {
            termDocs.close();
            termEnum.close();
        }
        valArray.seal();

        int doc = 0;
        while (doc <= maxDoc && orderArray.get(doc) != 0) {
            ++doc;
        }

        if (doc <= maxDoc) {
            minIDList.set(0, doc);
            // Try to get the max
            doc = maxDoc;
            while (doc > 0 && orderArray.get(doc) != 0) {
                --doc;
            }
            if (doc > 0) {
                maxIDList.set(0, doc);
            }
        }
        freqList.set(0, maxDoc + 1 - totalFreq);

        LazyBigIntArray freqs = new LazyBigIntArray(freqList.size());
        for(int i = 0; i < freqs.size(); i++) {
            freqs.add(i, freqList.get(i));
        }

        IntBuffer minIdsBuffer = ByteBuffer.allocateDirect(4 * minIDList.size()).asIntBuffer();
        minIdsBuffer.put(minIDList.toIntArray());

        IntBuffer maxIdsBuffer = ByteBuffer.allocateDirect(4 * maxIDList.size()).asIntBuffer();
        maxIdsBuffer.put(maxIDList.toIntArray());

//        IntBuffer freqListBuffer = ByteBuffer.allocateDirect(4 * freqList.size()).asIntBuffer();
//        freqListBuffer.put(freqList.toIntArray());

        return new FacetDataCache<T>(length, orderArray, valArray, freqs, minIdsBuffer, maxIdsBuffer, TermCountSize.large);
    }

    private static int[] convertString(FacetDataCache dataCache, String[] vals) {
        IntList list = new IntArrayList(vals.length);
        for (int i = 0; i < vals.length; ++i) {
            int index = dataCache.valArray.indexOf(vals[i]);
            if (index >= 0) {
                list.add(index);
            }
        }
        return list.toIntArray();
    }

    public int getFreqSize() {
        return freqs.size();
    }

    public TermValueList<T> getValArray() {
        return valArray;
    }

    public final String format(Object o) {
        return this.valArray.format(o);
    }

    public final T getRawValue(int idx) {
        return this.valArray.getRawValue(idx);
    }

    public final int getOrderArrayValue(int docId) {
        return orderArray.get(docId);
    }

    public final int getFreq(int docId) {
        return freqs.get(docId);
    }

    public final LazyBigIntArray getFreqs() {
        return freqs;
    }

    public final int getValArraySize() {
        return valArray.size();
    }

    public final int getOrderArraySize() {
        return length;
    }

    public final int getDocId(Object string) {
        return valArray.indexOf(string);
    }

    public final String getString(int val) {
        return valArray.get(val);
    }

    public final int getMinId(int doc) {
        return minIDs.get(doc);
    }

    public final int getMaxId(int doc) {
        return maxIDs.get(doc);
    }

    public int findValue(int value, int docId, int maxId) {
        for (int i = docId; i <= maxId; i++) {
            if (orderArray.get(i) == value)
                return i;
        }
        return DocIdSetIterator.NO_MORE_DOCS;
    }

    public int findValues(OpenBitSet bitset, int docId, int maxId) {
        for (int i = docId; i <= maxId; i++) {
            if (bitset.fastGet(orderArray.get(i)))
                return i;
        }
        return DocIdSetIterator.NO_MORE_DOCS;
    }

    public int findBits(int bits, int docId, int maxId) {
        for (int i = docId; i <= maxId; i++) {
            if ((orderArray.get(i) & bits) != 0)
                return i;
        }
        return DocIdSetIterator.NO_MORE_DOCS;
    }

    public int findValueRange(int minVal, int maxVal, int docId, int maxId)
    {
        for(int i = docId; i <= maxId; i++) {
            int val = orderArray.get(i);
            if(val >= minVal && val <= maxVal) return i;
        }
        return DocIdSetIterator.NO_MORE_DOCS;
    }

    public void close() {
      orderArray.limit(0);
      orderArray.clear();
      ((DirectBuffer) orderArray).cleaner().clean();
    }


    /**
     * Same as convert(FacetDataCache dataCache,String[] vals) except that the
     * values are supplied in raw form so that we can take advantage of the type
     * information to find index faster.
     *
     * @param <T>
     * @param dataCache
     * @param vals
     * @return the array of order indices of the values.
     */
    public static <T> int[] convert(FacetDataCache<T> dataCache, T[] vals) {
        if (vals != null && (vals instanceof String[]))
            return convertString(dataCache, (String[]) vals);
        IntList list = new IntArrayList(vals.length);
        for (int i = 0; i < vals.length; ++i) {
            int index = dataCache.valArray.indexOfWithType(vals[i]);
            if (index >= 0) {
                list.add(index);
            }
        }
        return list.toIntArray();
    }

    public static class FacetDocComparatorSource extends DocComparatorSource {
        private FacetHandler<FacetDataCache> _facetHandler;

        public FacetDocComparatorSource(FacetHandler<FacetDataCache> facetHandler) {
            _facetHandler = facetHandler;
        }

        @Override
        public DocComparator getComparator(IndexReader reader, int docbase) throws IOException {
            if (!(reader instanceof BoboIndexReader))
                throw new IllegalStateException("reader not instance of " + BoboIndexReader.class);
            BoboIndexReader boboReader = (BoboIndexReader) reader;
            final FacetDataCache dataCache = _facetHandler.getFacetData(boboReader);
            return new DocComparator() {

                @Override
                public Comparable value(ScoreDoc doc) {
                    int index = dataCache.getOrderArrayValue(doc.doc);
                    return dataCache.valArray.getComparableValue(index);
                }

                @Override
                public int compare(ScoreDoc doc1, ScoreDoc doc2) {
                    return dataCache.getOrderArrayValue(doc1.doc) - dataCache.getOrderArrayValue(doc2.doc);
                }
            };
        }
    }
}
