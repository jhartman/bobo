package com.browseengine.bobo.facets.data;

import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.util.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.OpenBitSet;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BoboIndexReader.WorkArea;
import com.browseengine.bobo.facets.range.MultiDataCacheBuilder;
import com.browseengine.bobo.sort.DocComparator;
import com.browseengine.bobo.sort.DocComparatorSource;
import com.browseengine.bobo.util.BigNestedIntArray.BufferedLoader;
import com.browseengine.bobo.util.BigNestedIntArray.Loader;

public class MultiValueWithWeightFacetDataCache<T> extends MultiValueFacetDataCache<T>
{
  private static final long serialVersionUID = 1L;

  private final NioMatrix _weightArray;

    public MultiValueWithWeightFacetDataCache(int length, NioMatrix nestedArray,
                                                    NioMatrix weightArray,
                                                  TermValueList<T> valArray,
                                                  LazyBigIntArray freqs,
                                                  IntBuffer minIdsBuffer,
                                                  IntBuffer maxIdsBuffer, FacetHandler.TermCountSize large) {
        super(length, nestedArray, valArray, freqs, minIdsBuffer, maxIdsBuffer, large);
        _weightArray = weightArray;
    }

  public int getWeight(int docId, int column) {
    return _weightArray.get(docId, column);
  }

  public int getWeightForValue(int docId, int value, int defaultIfNotFound) {
      if(docId >= 0 && docId < getNestedArraySize()) {
          int column = binarySearch(docId, value);
          if(column >= 0)
            return getWeight(docId, column);
          else
            return defaultIfNotFound;
      } else {
          return defaultIfNotFound;
      }
  }

  /**
   * loads multi-value facet data. This method uses a workarea to prepare loading.
   * @param fieldName
   * @param reader
   * @param listFactory
   * @param workArea
   * @throws IOException
   */
  public static <T> MultiValueWithWeightFacetDataCache<T> load(String fieldName,
                   IndexReader reader,
                   TermListFactory<T> listFactory,
                   WorkArea workArea,
                   int maxItems) throws IOException
  {
    long t0 = System.currentTimeMillis();
    int maxdoc = reader.maxDoc();

//    BigNestedIntArray nestedArray = new BigNestedIntArray();
//    nestedArray.setMaxItems(maxItems);

      NioMatrix nestedArray = new NioMatrix(maxdoc + 1);

      NioMatrix weightArray = new NioMatrix(maxdoc + 1);

    TermEnum tenum = null;
    TermDocs tdoc = null;
    TermValueList<T> valArray = (listFactory == null ? (TermValueList<T>)new TermStringList() : listFactory.createTermList());
    IntArrayList minIDList = new IntArrayList();
    IntArrayList maxIDList = new IntArrayList();
    IntArrayList freqList = new IntArrayList();
    OpenBitSet bitset = new OpenBitSet(maxdoc + 1);
    int negativeValueCount = getNegativeValueCount(reader, fieldName.intern());
    int t = 0; // current term number
    valArray.add(null);
    minIDList.add(-1);
    maxIDList.add(-1);
    freqList.add(0);
    t++;

    boolean overflow = false;

    String pre = null;

    int df = 0;
    int minID = -1;
    int maxID = -1;
    int valId = 0;

    try
    {
      tdoc = reader.termDocs();
      tenum = reader.terms(new Term(fieldName, ""));
      if (tenum != null)
      {
        do
        {
          Term term = tenum.term();
          if (term == null || !fieldName.equals(term.field()))
            break;

          String val = term.text();

          if (val != null)
          {
            int weight = 0;
            String[] split = val.split("\u0000");
            if (split.length > 1)
            {
              val = split[0];
              weight = Integer.parseInt(split[split.length-1]);
            }
            if (pre == null || !val.equals(pre))
            {
              if (pre != null)
              {
                freqList.add(df);
                minIDList.add(minID);
                maxIDList.add(maxID);
              }

              valArray.add(val);

              df = 0;
              minID = -1;
              maxID = -1;
              valId = (t - 1 < negativeValueCount) ? (negativeValueCount - t + 1) : t;
              t++;
            }

            tdoc.seek(tenum);
            if(tdoc.next())
            {
              df++;
              int docid = tdoc.doc();

              nestedArray.add(docid, valId);
              weightArray.add(docid, weight);

              if (docid < minID) minID = docid;
              bitset.fastSet(docid);
              while(tdoc.next())
              {
                df++;
                docid = tdoc.doc();

                nestedArray.add(docid, valId);
                weightArray.add(docid, weight);

                bitset.fastSet(docid);
              }
              if (docid > maxID) maxID = docid;
            }
            pre = val;
          }

        }
        while (tenum.next());
        if (pre != null)
        {
          freqList.add(df);
          minIDList.add(minID);
          maxIDList.add(maxID);
        }
      }
    }
    finally
    {
      try
      {
        if (tdoc != null)
        {
          tdoc.close();
        }
      }
      finally
      {
        if (tenum != null)
        {
          tenum.close();
        }
      }
    }

    valArray.seal();

    int doc = 0;
    while (doc <= maxdoc && !contains(nestedArray, doc, 0, true))
    {
      ++doc;
    }
    if (doc <= maxdoc)
    {
      minIDList.set(0, doc);
      doc = maxdoc;
      while (doc > 0 && !contains(nestedArray, doc, 0, true))
      {
        --doc;
      }
      if (doc > 0)
      {
        maxIDList.set(0, doc);
      }
    }
    freqList.set(0, maxdoc + 1 - (int) bitset.cardinality());

    IntBuffer minIdsBuffer = ByteBuffer.allocateDirect(4 * minIDList.size()).asIntBuffer();
    minIdsBuffer.put(minIDList.toIntArray());

    IntBuffer maxIdsBuffer = ByteBuffer.allocateDirect(4 * maxIDList.size()).asIntBuffer();
    maxIdsBuffer.put(maxIDList.toIntArray());

//    IntBuffer freqListBuffer = ByteBuffer.allocateDirect(4 * freqList.size()).asIntBuffer();
//    freqListBuffer.put(freqList.toIntArray());

      LazyBigIntArray freqs = new LazyBigIntArray(freqList.size());
      for(int i = 0; i < freqs.size(); i++) {
          freqs.add(i, freqList.get(i));
      }

      nestedArray.compact();

      return new MultiValueWithWeightFacetDataCache<T>(1+maxdoc, nestedArray, weightArray, valArray, freqs, minIdsBuffer, maxIdsBuffer, FacetHandler.TermCountSize.large);
  }
}
