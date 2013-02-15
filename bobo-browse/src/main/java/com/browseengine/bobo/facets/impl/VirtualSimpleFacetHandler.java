package com.browseengine.bobo.facets.impl;

import com.browseengine.bobo.facets.data.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import com.browseengine.bobo.util.LazyBigIntArray;
import org.apache.lucene.index.TermDocs;

import com.browseengine.bobo.api.BoboIndexReader;

public class VirtualSimpleFacetHandler extends SimpleFacetHandler
{
  protected FacetDataFetcher _facetDataFetcher;

  public VirtualSimpleFacetHandler(String name,
                                   String indexFieldName,
                                   TermListFactory termListFactory,
                                   FacetDataFetcher facetDataFetcher,
                                   Set<String> dependsOn)
  {
    super(name, null, termListFactory, dependsOn);
    _facetDataFetcher = facetDataFetcher;
  }

  public VirtualSimpleFacetHandler(String name,
                                   TermListFactory termListFactory,
                                   FacetDataFetcher facetDataFetcher,
                                   Set<String> dependsOn)
  {
    this(name, null, termListFactory, facetDataFetcher, dependsOn);
  }

  @Override
  public FacetDataCache load(BoboIndexReader reader) throws IOException
  {
    int doc = -1;
    SortedMap<Object, LinkedList<Integer>> dataMap = null;
    LinkedList<Integer> docList = null;

    int nullMinId = -1;
    int nullMaxId = -1;
    int nullFreq = 0;

    TermDocs termDocs = reader.termDocs(null);
    try
    {
      while(termDocs.next())
      {
        doc = termDocs.doc();
        Object val = _facetDataFetcher.fetch(reader, doc);
        if (val == null)
        {
          if (nullMinId < 0)
            nullMinId = doc;
          nullMaxId = doc;
          ++ nullFreq;
          continue;
        }
        if (dataMap == null)
        {
          // Initialize.
          if (val instanceof long[])
          {
            if(_termListFactory == null)
              _termListFactory = new TermFixedLengthLongArrayListFactory(
                ((long[])val).length);

            dataMap = new TreeMap<Object, LinkedList<Integer>>(new Comparator<Object>()
            {
              public int compare(Object big, Object small)
              {
                if (((long[])big).length != ((long[])small).length)
                {
                  throw new RuntimeException(""+Arrays.asList(((long[])big))+" and "+
                    Arrays.asList(((long[])small))+" have different length.");
                }

                long r = 0;
                for (int i=0; i<((long[])big).length; ++i)
                {
                  r = ((long[])big)[i] - ((long[])small)[i];
                  if (r != 0)
                    break;
                }

                if (r > 0)
                  return 1;
                else if (r < 0)
                  return -1;

                return 0;
              }
            });
          }
          else if (val instanceof Comparable)
          {
            dataMap = new TreeMap<Object, LinkedList<Integer>>();
          }
          else
          {
            dataMap = new TreeMap<Object, LinkedList<Integer>>(new Comparator<Object>()
            {
              public int compare(Object big, Object small)
              {
                return String.valueOf(big).compareTo(String.valueOf(small));
              }
            });
          }
        }

        docList = dataMap.get(val);
        if (docList == null)
        {
          docList = new LinkedList<Integer>();
          dataMap.put(val, docList);
        }
        docList.add(doc);
      }
    }
    finally
    {
      termDocs.close();
    }

    int maxDoc = reader.maxDoc();
    int size = dataMap == null ? 1:(dataMap.size() + 1);

      IntBuffer order = ByteBuffer.allocateDirect(4 * (1 + maxDoc)).asIntBuffer();
      TermValueList list = _termListFactory == null ?
      new TermStringList(size) :
      _termListFactory.createTermList(size);

    int[] freqs = new int[size];
    int[] minIDs = new int[size];
    int[] maxIDs = new int[size];

    list.add(null);
    freqs[0] = nullFreq;
    minIDs[0] = nullMinId;
    maxIDs[0] = nullMaxId;

    if (dataMap != null)
    {
      int i = 1;
      Integer docId;
      for (Map.Entry<Object, LinkedList<Integer>> entry : dataMap.entrySet())
      {
        list.addRaw(entry.getKey());
        docList = entry.getValue();
        freqs[i] = docList.size();
        minIDs[i] = docList.get(0);
        while((docId = docList.poll()) != null)
        {
          doc = docId;
          order.put(doc, i);
        }
        maxIDs[i] = doc;
        ++i;
      }
    }
    list.seal();

      IntBuffer minIdsBuffer = ByteBuffer.allocateDirect(4 * minIDs.length).asIntBuffer();
      minIdsBuffer.put(minIDs);

      IntBuffer maxIdsBuffer = ByteBuffer.allocateDirect(4 * maxIDs.length).asIntBuffer();
      maxIdsBuffer.put(maxIDs);

//      IntBuffer freqListBuffer = ByteBuffer.allocateDirect(4 * freqs.length).asIntBuffer();
//      freqListBuffer.put(freqs);

      LazyBigIntArray lazyFreqs = new LazyBigIntArray(freqs.length);
      for(int i = 0; i < freqs.length; i++) {
          lazyFreqs.add(i, freqs[i]);
      }

      FacetDataCache dataCache = new FacetDataCache(1+maxDoc, order, list, lazyFreqs, minIdsBuffer, maxIdsBuffer, TermCountSize.large);
    return dataCache;
  }

  /**
   * @see com.browseengine.bobo.facets.FacetHandler#cleanup
   */
  @Override
  public void cleanup(BoboIndexReader reader)
  {
    _facetDataFetcher.cleanup(reader);
  }
}
