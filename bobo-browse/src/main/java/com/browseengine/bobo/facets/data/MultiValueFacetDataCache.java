/**
 * 
 */
package com.browseengine.bobo.facets.data;

import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.query.scoring.FacetTermScoringFunction;
import com.browseengine.bobo.util.*;
import it.unimi.dsi.fastutil.chars.CharOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;

import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.shorts.ShortOpenHashSet;
import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.OpenBitSet;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BoboIndexReader.WorkArea;
import com.browseengine.bobo.facets.range.MultiDataCacheBuilder;
import com.browseengine.bobo.sort.DocComparator;
import com.browseengine.bobo.sort.DocComparatorSource;
import com.browseengine.bobo.util.BigNestedIntArray.BufferedLoader;
import com.browseengine.bobo.util.BigNestedIntArray.Loader;

/**
 * @author ymatsuda
 *
 */
public class MultiValueFacetDataCache<T> extends FacetDataCache<T>
{
  private static final long serialVersionUID = 1L;
  private static Logger logger = Logger.getLogger(MultiValueFacetDataCache.class);
 
    private final NioMatrix _nestedArray;

//  protected final boolean _overflow;

    public MultiValueFacetDataCache(int length, NioMatrix nestedArray,
                                    TermValueList<T> valArray,
                                    LazyBigIntArray freqs,
                                    IntBuffer minIdsBuffer,
                                    IntBuffer maxIdsBuffer, FacetHandler.TermCountSize termCountSize) {
        super(length, null, valArray, freqs, minIdsBuffer, maxIdsBuffer, termCountSize);
        _nestedArray = nestedArray;
    }

//  public MultiValueFacetDataCache<T> setMaxItems(int maxItems)
//  {
//    _maxItems = Math.min(maxItems, BigNestedIntArray.MAX_ITEMS);
//    _nestedArray.setMaxItems(_maxItems);
//    return this;
//  }
  
  @Override
  public int getNumItems(int docid){
      return _nestedArray.getColumnLength(docid);
  }

  /**
   * loads multi-value facet data. This method uses a workarea to prepare loading.
   * @param fieldName
   * @param reader
   * @param listFactory
   * @param workArea
   * @throws IOException
   */
  public static <T> MultiValueFacetDataCache<T> load(String fieldName, IndexReader reader, TermListFactory<T> listFactory, WorkArea workArea, int maxItems) throws IOException
  {
    long t0 = System.currentTimeMillis();
    int maxdoc = reader.maxDoc();

    boolean overFlow = false;
    NioMatrix nestedArray = new NioMatrix(maxdoc + 1);


//    BigNestedIntArray nestedArray = new BigNestedIntArray();
//    maxItems = Math.min(maxItems, BigNestedIntArray.MAX_ITEMS);
//    nestedArray.setMaxItems(maxItems);

//    BufferedLoader loader = getBufferedLoader(maxdoc, maxItems, workArea);

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
            valArray.add(val);

            tdoc.seek(tenum);
            //freqList.add(tenum.docFreq()); // removed because the df doesn't take into account the num of deletedDocs
            int df = 0;
            int minID = -1;
            int maxID = -1;
            int valId = (t - 1 < negativeValueCount) ? (negativeValueCount - t + 1) : t;
            if(tdoc.next())
            {
              df++;
              int docid = tdoc.doc();
              nestedArray.add(docid, valId);
              minID = docid;
              bitset.fastSet(docid);
              while(tdoc.next())
              {
                df++;
                docid = tdoc.doc();

                nestedArray.add(docid, valId);
                bitset.fastSet(docid);
              }
              maxID = docid;
            }
            freqList.add(df);
            minIDList.add(minID);
            maxIDList.add(maxID);
          }

          t++;
        }
        while (tenum.next());
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

//    try
//    {
//      nestedArray.load(maxdoc + 1, loader);
//    }
//    catch (IOException e)
//    {
//      throw e;
//    }
//    catch (Exception e)
//    {
//      throw new RuntimeException("failed to load due to " + e.toString(), e);
//    }

//    for(int i = 0; i < inMemoryNestedArray.length; i++) {
//        Arrays.sort(inMemoryNestedArray[i].elements());
//    }

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

      return new MultiValueFacetDataCache<T>(1+maxdoc, nestedArray, valArray, freqs, minIdsBuffer, maxIdsBuffer, FacetHandler.TermCountSize.large);
  }

  public static boolean contains(NioMatrix matrix, int doc, int value, boolean withMissing) {
      int index = matrix.binarySearch(doc, value);
      if(index >= 0)
          return true;
      else {
          return withMissing && (matrix.getColumnLength(doc) == 0) && (value == 0);
      }
  }

  /**
   * loads multi-value facet data. This method uses the count payload to allocate storage before loading data.
   * @param fieldName
   * @param sizeTerm
   * @param reader
   * @param listFactory
   * @throws IOException
   */
  public static <T> MultiValueFacetDataCache<T> load(String fieldName, IndexReader reader, TermListFactory<T> listFactory, Term sizeTerm, int maxItems) throws IOException
  {
//    BigNestedIntArray nestedArray = new BigNestedIntArray();
    maxItems = Math.min(maxItems, BigNestedIntArray.MAX_ITEMS);
//    nestedArray.setMaxItems(maxItems);



    int maxdoc = reader.maxDoc();

    NioMatrix nestedArray = new NioMatrix(maxdoc + 1);
//      Loader loader = new AllocOnlyLoader(maxItems, sizeTerm, reader);
    int negativeValueCount = getNegativeValueCount(reader, fieldName.intern()); 
//    try
//    {
//      nestedArray.load(maxdoc + 1, loader);
//    }
//    catch (IOException e)
//    {
//      throw e;
//    }
//    catch (Exception e)
//    {
//      throw new RuntimeException("failed to load due to " + e.toString(), e);
//    }
    
    TermEnum tenum = null;
    TermDocs tdoc = null;
    TermValueList<T> valArray = (listFactory == null ? (TermValueList<T>)new TermStringList() : listFactory.createTermList());
    IntArrayList minIDList = new IntArrayList();
    IntArrayList maxIDList = new IntArrayList();
    IntArrayList freqList = new IntArrayList();
    OpenBitSet bitset = new OpenBitSet(maxdoc + 1);

    int t = 0; // current term number
    valArray.add(null);
    minIDList.add(-1);
    maxIDList.add(-1);
    freqList.add(0);
    t++;

    boolean overflow = false;
    try
    {
      tdoc = reader.termDocs();
      tenum = reader.terms(new Term(fieldName, ""));
      if (tenum != null)
      {
        do
        {
          Term term = tenum.term();
          if(term == null || !fieldName.equals(term.field()))
            break;
          
          String val = term.text();
          
          if (val != null)
          {
            valArray.add(val);
            
            tdoc.seek(tenum);
            //freqList.add(tenum.docFreq()); // removed because the df doesn't take into account the num of deletedDocs
            int df = 0;
            int minID = -1;
            int maxID = -1;
            if(tdoc.next())
            {
              df++;
              int docid = tdoc.doc();
              nestedArray.add(docid, t);
              minID = docid;
              bitset.fastSet(docid);
              int valId = (t - 1 < negativeValueCount) ? (negativeValueCount - t + 1) : t;
              while(tdoc.next())
              {
                df++;
                docid = tdoc.doc();
                nestedArray.add(docid, valId);
                bitset.fastSet(docid);
              }
              maxID = docid;
            }
            freqList.add(df);
            minIDList.add(minID);
            maxIDList.add(maxID);
          }
          
          t++;
        }
        while (tenum.next());
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
      while (doc > 0 && !!contains(nestedArray, doc, 0, true))
      {
        --doc;
      }
      if (doc > 0)
      {
        maxIDList.set(0, doc);
      }
    }
    freqList.set(0,  maxdoc + 1 - (int) bitset.cardinality());

      IntBuffer minIdsBuffer = ByteBuffer.allocateDirect(4 * minIDList.size()).asIntBuffer();
      minIdsBuffer.put(minIDList.toIntArray());

      IntBuffer maxIdsBuffer = ByteBuffer.allocateDirect(4 * maxIDList.size()).asIntBuffer();
      maxIdsBuffer.put(maxIDList.toIntArray());

//      IntBuffer freqListBuffer = ByteBuffer.allocateDirect(4 * freqList.size()).asIntBuffer();
//      freqListBuffer.put(freqList.toIntArray());

      LazyBigIntArray freqs = new LazyBigIntArray(freqList.size());
      for(int i = 0; i < freqs.size(); i++) {
          freqs.add(i, freqList.get(i));
      }

      nestedArray.compact();

      return new MultiValueFacetDataCache<T>(1+maxdoc, nestedArray, valArray, freqs, minIdsBuffer, maxIdsBuffer, FacetHandler.TermCountSize.large);
  }

  private static final String[] EMPTY = new String[0];

  public String[] getTranslatedData(int id) {
      if(id < _nestedArray.getRowLength()) {
          String[] results = new String[_nestedArray.getColumnLength(id)];
          for(int i = 0; i < results.length; i++) {
              results[i] = getString(_nestedArray.get(id, i));
          }
          return results;
      }
      return EMPTY;
  }

  public Object[] getRawData(int id) {
      if(id < _nestedArray.getRowLength()) {
          Object[] results = new Object[_nestedArray.getColumnLength(id)];
          for(int i = 0; i < results.length; i++) {
              results[i] = super.getRawValue(_nestedArray.get(id, i));
          }
          return results;
      }

      return EMPTY;
  }

  public DocIdSetIterator getIterator(final int id) {
    return new DocIdSetIterator() {
        int pointer = 0;
        final int columnLength = _nestedArray.getColumnLength(id);
        @Override
        public int docID() {
          if(pointer < columnLength) {
              return _nestedArray.get(id, pointer);
          } else {
              return DocIdSetIterator.NO_MORE_DOCS;
          }
        }

        @Override
        public int nextDoc() throws IOException {
            if(pointer < columnLength) {
                int result = _nestedArray.get(id, pointer);
                pointer++;
                return result;
            } else {
                return DocIdSetIterator.NO_MORE_DOCS;
            }
        }

        @Override
        public int advance(int id) throws IOException {
          int currentId = docID();

          // TODO: We could binary search here, but there are usually not that many values per multi value facet.
          while(currentId < id && currentId != DocIdSetIterator.NO_MORE_DOCS) {
             currentId = nextDoc();
          }
          return currentId;
        }
    };
  }

  public boolean containsSet(Object set, int docId) {
      return getSet(set, docId) >= 0;
  }

  public int getSet(Object set, int docId) {
      int columnLength = _nestedArray.getColumnLength(docId);
      if(valArray instanceof TermIntList) {
          IntOpenHashSet values = (IntOpenHashSet) set;
          for(int i = 0; i < columnLength; i++) {
              int value = ((TermIntList) valArray).getPrimitiveValue(_nestedArray.get(docId, i));
              if(values.contains(value))
                  return i;
          }
          return -1;
      } else if (valArray instanceof TermLongList) {
          LongOpenHashSet values = (LongOpenHashSet) set;
          for(int i = 0; i < columnLength; i++) {
              long value = ((TermLongList) valArray).getPrimitiveValue(_nestedArray.get(docId, i));
              if(values.contains(value))
                  return i;
          }
          return -1;
      } else if (valArray instanceof TermCharList) {
          CharOpenHashSet values = (CharOpenHashSet) set;
          for(int i = 0; i < columnLength; i++) {
              char value = ((TermCharList) valArray).getRawValue(_nestedArray.get(docId, i));
              if(values.contains(value))
                  return i;
          }
          return -1;
      } else if (valArray instanceof TermShortList) {
          ShortOpenHashSet values = (ShortOpenHashSet) set;
          for(int i = 0; i < columnLength; i++) {
              short value = ((TermShortList) valArray).getPrimitiveValue(_nestedArray.get(docId, i));
              if(values.contains(value))
                  return i;
          }
          return -1;
      }  else if (valArray instanceof TermFloatList) {
          FloatOpenHashSet values = (FloatOpenHashSet) set;
          for(int i = 0; i < columnLength; i++) {
              float value = ((TermFloatList) valArray).getPrimitiveValue(_nestedArray.get(docId, i));
              if(values.contains(value))
                  return i;
          }
          return -1;
      } else if (valArray instanceof TermDoubleList) {
          DoubleOpenHashSet values = (DoubleOpenHashSet) set;
          for(int i = 0; i < columnLength; i++) {
              double value = ((TermDoubleList) valArray).getPrimitiveValue(_nestedArray.get(docId, i));
              if(values.contains(value))
                  return i;
          }
          return -1;
      } else if (valArray instanceof TermStringList) {
          ObjectOpenHashSet values = (ObjectOpenHashSet) set;
          for(int i = 0; i < columnLength; i++) {
              String value = ((TermStringList) valArray).getRawValue(_nestedArray.get(docId, i));
              if(values.contains(value))
                  return i;
          }
          return -1;
      } else {
          throw new IllegalArgumentException();
      }
  }

  public void countNoReturn(int docId, BigSegmentedArray count) {
    if(docId < _nestedArray.getRowLength()) {
      int columnLength = _nestedArray.getColumnLength(docId);
      if(columnLength == 0)
        count.add(0, count.get(0) + 1);
      else {
          for(int i = 0; i < _nestedArray.getColumnLength(docId); i++) {
            int val = _nestedArray.get(docId, i);
            count.add(val, count.get(val) + 1);
          }
      }
    }
  }

    @Override
    public final int findValue(int value, int docId, int maxId) {
        while(docId <= maxId && docId < _nestedArray.getRowLength()) {
            int index = _nestedArray.binarySearch(docId, value);
            if(index >= 0)
                return docId;
            docId++;
        }
        return DocIdSetIterator.NO_MORE_DOCS;
    }

    public final int findValue(int value, int docId, int maxId, boolean orMissing) {
        while(docId <= maxId && docId < _nestedArray.getRowLength()) {
            int index = _nestedArray.binarySearch(docId, value);
            if(index >= 0)
                return docId;
            else if(orMissing && (_nestedArray.getColumnLength(docId) == 0) && (value == 0))
                return docId;
            docId++;
        }
        return DocIdSetIterator.NO_MORE_DOCS;
    }

    @Override
    public final int findValues(OpenBitSet bitset, int docId, int maxId) {
        while(docId <= maxId && docId < _nestedArray.getRowLength()) {
          for(int i = 0; i < _nestedArray.getColumnLength(docId); i++) {
              if(bitset.fastGet(_nestedArray.get(docId, i)))
                return docId;
          }

          docId++;
        }
        return DocIdSetIterator.NO_MORE_DOCS;
    }



    // TODO: IS THIS THE SAME AS findValuesInRange???

    @Override
    public final int findValueRange(int minVal, int maxVal, int docId, int maxId)
    {
      return findValuesInRange(minVal, maxVal, docId, maxId);
    }

    public int findValuesInRange(int minVal, int maxVal, int docId, int maxId) {
        if(minVal <= maxVal) {
            while(docId <= maxId && docId < _nestedArray.getRowLength()) {
                int minIndex = _nestedArray.binarySearch(docId, minVal);

                int maxIndex = _nestedArray.binarySearch(docId, maxVal);
                if(minIndex >= 0 || maxIndex >= 0)
                    return docId;

                minIndex = -minIndex - 1;
                maxIndex = -maxIndex - 1;

                // So technically you don't need a for loop here, but just for convenience I did it. The binary searches
                // should completely resolve whether or not there's a value in the range
                for(int i = minIndex; i < maxIndex && i < _nestedArray.getColumnLength(docId); i++) {
                    int nestedArrayValue = _nestedArray.get(docId, i);
                    if(minVal <= nestedArrayValue && nestedArrayValue <= maxVal)
                        return docId;
                }

                docId++;
            }
        }

        return DocIdSetIterator.NO_MORE_DOCS;
    }

    public int binarySearch(int id, int value) {
        return _nestedArray.binarySearch(id, value);
    }

    public boolean contains(int id, int value) {
        return id < _nestedArray.getRowLength() && _nestedArray.binarySearch(id, value) >= 0;
    }

    public boolean contains(int id, OpenBitSet value) {
        if(id < _nestedArray.getRowLength()) {
            for(int i = 0; i < _nestedArray.getColumnLength(id); i++) {
                if(value.fastGet(_nestedArray.get(id, i)))
                    return true;
            }
        }

        return false;
    }



    // This one is exclusive?!?
    public boolean containsValueInRange(int docId, int minVal, int maxVal) {
        if(docId < _nestedArray.getRowLength()) {
            int minIndex = _nestedArray.binarySearch(docId, minVal);
            int maxIndex = _nestedArray.binarySearch(docId, maxVal);

            minIndex = minIndex > 0 ? minIndex : -minIndex - 1;
            maxIndex = maxIndex > 0 ? maxIndex : -maxIndex - 1;

            // So technically you don't need a for loop here, but just for convenience I did it. The binary searches
            // should completely resolve whether or not there's a value in the range
            for(int i = minIndex + 1; i <= maxIndex; i++) {
                if(minVal <= _nestedArray.get(docId, i) && _nestedArray.get(docId, i) < maxVal)
                    return true;
            }

        }
        return false;
    }

  protected static boolean logOverflow(boolean overflow, int maxItems, String fieldName)
  {
    if (!overflow)
    {
      logger.error("Maximum value per document: " + maxItems + " exceeded, fieldName=" + fieldName);
      return true;
    }
    return false;
  }

  protected static BufferedLoader getBufferedLoader(int maxdoc, int maxItems, WorkArea workArea)
  {
    if(workArea == null)
    {
      return new BufferedLoader(maxdoc, maxItems, new BigIntBuffer());
    }
    else
    {
      BigIntBuffer buffer = workArea.get(BigIntBuffer.class);
      if(buffer == null)
      {
        buffer = new BigIntBuffer();
        workArea.put(buffer);
      }
      else
      {
        buffer.reset();
      }
      
      BufferedLoader loader = workArea.get(BufferedLoader.class);      
      if(loader == null || loader.capacity() < maxdoc)
      {
        loader = new BufferedLoader(maxdoc, maxItems, buffer);
        workArea.put(loader);
      }
      else
      {
        loader.reset(maxdoc, maxItems, buffer);
      }
      return loader;
    }
  }

    public int getNestedArraySize() {
        return _nestedArray.getRowLength();
    }

    public float getScores(int docid, LazyBigIntArray freqs, float[] boosts, FacetTermScoringFunction function) {
        function.clearScores();
        if(docid < _nestedArray.getRowLength()) {
           for(int i = 0; i < _nestedArray.getColumnLength(docid); i++) {
               int idx = _nestedArray.get(docid, i);
               function.scoreAndCollect(freqs.get(idx), boosts[idx]);
           }
        }
        return function.getCurrentScore();
    }



    /**
   * A loader that allocate data storage without loading data to BigNestedIntArray.
   * Note that this loader supports only non-negative integer data.
   */
  public final static class AllocOnlyLoader extends Loader
  {
    private IndexReader _reader;
    private Term _sizeTerm;
    private int _maxItems;

    public AllocOnlyLoader(int maxItems, Term sizeTerm, IndexReader reader) throws IOException
    {
      _maxItems = Math.min(maxItems, BigNestedIntArray.MAX_ITEMS);
      _sizeTerm = sizeTerm;
      _reader = reader;
    }

    @Override
    public void load() throws Exception
    {
      TermPositions tp = null;
      byte[] payloadBuffer = new byte[4];        // four bytes for an int
      try
      {
        tp = _reader.termPositions(_sizeTerm);

        if(tp == null) return;

        while(tp.next())
        {
          if(tp.freq() > 0)
          {
            tp.nextPosition();
            tp.getPayload(payloadBuffer, 0);
            int len = bytesToInt(payloadBuffer);
            allocate(tp.doc(), Math.min(len, _maxItems), true);
          }
        }
      }
      finally
      {
        if(tp != null) tp.close();
      }
    }

    private static int bytesToInt(byte[] bytes)
    {
      return ((bytes[3] & 0xFF) << 24) | ((bytes[2] & 0xFF) << 16) |
              ((bytes[1] & 0xFF) <<  8) |  (bytes[0] & 0xFF);
    }
  }
    
	public final static class MultiFacetDocComparatorSource extends DocComparatorSource{
		private MultiDataCacheBuilder cacheBuilder;
		public MultiFacetDocComparatorSource(MultiDataCacheBuilder multiDataCacheBuilder){
		  cacheBuilder = multiDataCacheBuilder;
		}
		
		@Override
		public DocComparator getComparator(final IndexReader reader, int docbase)
				throws IOException {
			if (!(reader instanceof BoboIndexReader)) throw new IllegalStateException("reader must be instance of "+BoboIndexReader.class);
			BoboIndexReader boboReader = (BoboIndexReader)reader;
			final MultiValueFacetDataCache dataCache = cacheBuilder.build(boboReader);
			return new DocComparator(){
				
				@Override
				public int compare(ScoreDoc doc1, ScoreDoc doc2) {
                    int col1Len = dataCache._nestedArray.getColumnLength(doc1.doc);
                    int col2Len = dataCache._nestedArray.getColumnLength(doc2.doc);

                    if(col1Len == 0 && col2Len == 0)
                        return 0;
                    else if(col1Len == 0 && col2Len > 0) {
                        return -1;
                    } else if(col1Len > 0 && col2Len == 0) {
                        return 1;
                    } else {
                        for(int col = 0; col < col1Len && col < col2Len; col++) {
                            int val1 = dataCache._nestedArray.get(doc1.doc, col);
                            int val2 = dataCache._nestedArray.get(doc2.doc, col);
                            if(val1 != val2) return val1 - val2;
                        }

                        if(col1Len < col2Len)
                          return -1;
                        else if(col1Len == col2Len)
                          return 0;
                        else
                          return 1;
                    }
				}

				@Override
				public Comparable value(ScoreDoc doc) {

                    String[] vals = dataCache.getTranslatedData(doc.doc);
                    return new StringArrayComparator(vals);
				}
				
			};
		}
	}
}
