package com.browseengine.bobo.facets.filter;

import java.io.IOException;

import com.browseengine.bobo.facets.data.FacetDataCache;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.OpenBitSet;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.docidset.EmptyDocIdSet;
import com.browseengine.bobo.docidset.RandomAccessDocIdSet;
import com.browseengine.bobo.facets.FacetHandler;
//import com.browseengine.bobo.facets.data.FacetDataCache;


public class FacetOrFilter extends RandomAccessFilter
{
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  
  protected final FacetHandler<FacetDataCache> _facetHandler;
  protected final String[] _vals;
  private final boolean _takeCompliment;
  private final FacetValueConverter _valueConverter;
  
  public FacetOrFilter(FacetHandler<FacetDataCache> facetHandler, String[] vals)
  {
    this(facetHandler,vals,false);
  }
  
  public FacetOrFilter(FacetHandler<FacetDataCache> facetHandler, String[] vals,boolean takeCompliment){
	this(facetHandler,vals,takeCompliment,FacetValueConverter.DEFAULT);  
  }
  
  public FacetOrFilter(FacetHandler<FacetDataCache> facetHandler, String[] vals,boolean takeCompliment,FacetValueConverter valueConverter)
  {
    _facetHandler = facetHandler;
    _vals = vals;
    _takeCompliment = takeCompliment;
    _valueConverter = valueConverter;
  }
  
  public double getFacetSelectivity(BoboIndexReader reader)
  {
    double selectivity = 0;
    FacetDataCache dataCache = _facetHandler.getFacetData(reader);
    int accumFreq=0;
    for(String value : _vals)
    {
      int idx = dataCache.getDocId(value);
      if(idx < 0)
      {
        continue;
      }
      accumFreq +=dataCache.getFreq(idx);
    }
    int total = reader.maxDoc();
    selectivity = (double)accumFreq/(double)total;
    if(selectivity > 0.999)
    {
      selectivity = 1.0;
    }
    if(_takeCompliment)
    {
      selectivity = 1.0 - selectivity;
    }
    return selectivity;
  }
  
  @Override
  public RandomAccessDocIdSet getRandomAccessDocIdSet(BoboIndexReader reader) throws IOException
  {
    if (_vals.length == 0)
    {
      return EmptyDocIdSet.getInstance();
    }
    else
    {
    	return new FacetOrRandomAccessDocIdSet(_facetHandler, reader, _vals, _valueConverter,_takeCompliment);
    }
  }
  
  public static class FacetOrRandomAccessDocIdSet extends RandomAccessDocIdSet{

	private OpenBitSet _bitset;
	private final FacetDataCache _dataCache;
	private final int[] _index;
	
	FacetOrRandomAccessDocIdSet(FacetHandler<FacetDataCache> facetHandler,BoboIndexReader reader,
								String[] vals,FacetValueConverter valConverter,boolean takeCompliment){
		_dataCache = facetHandler.getFacetData(reader);
	    _index = valConverter.convert(_dataCache, vals);
	    
	    _bitset = new OpenBitSet(_dataCache.getValArraySize());
	    for (int i : _index)
	    {
	      _bitset.fastSet(i);
	    }
      
      if (takeCompliment)
      {
        // flip the bits
        for (int i=0; i < _dataCache.getValArraySize();++i){
          _bitset.fastFlip(i);
        }
      }
	}
	
	@Override
	public boolean get(int docId) {
		return _bitset.fastGet(_dataCache.getOrderArrayValue(docId));
	}

	@Override
	public DocIdSetIterator iterator() throws IOException {
        return new FacetOrDocIdSetIterator(_dataCache,_bitset);
	}
	  
  }
  
  public static class FacetOrDocIdSetIterator extends DocIdSetIterator
  {
      protected int _doc;
      protected final FacetDataCache _dataCache;
      protected int _maxID;
      protected final OpenBitSet _bitset;

      public FacetOrDocIdSetIterator(FacetDataCache dataCache, OpenBitSet bitset)
      {
          _dataCache=dataCache;
          _bitset=bitset;
              
          _doc = Integer.MAX_VALUE;
          _maxID = -1;
          int size = _dataCache.getValArraySize();
          for (int i=0;i<size;++i) {
            if (!bitset.fastGet(i)){
              continue;
            }
            if (_doc > _dataCache.getMinId(i)){
              _doc = _dataCache.getMinId(i);
            }
            if (_maxID < _dataCache.getMaxId(i))
            {
              _maxID = _dataCache.getMaxId(i);
            }
          }
          _doc--;
          if (_doc<0) _doc=-1;
      }
      
      @Override
      final public int docID() {
          return _doc;
      }
      
      @Override
      public int nextDoc() throws IOException
      {
        return (_doc = (_doc < _maxID ? _dataCache.findValues(_bitset, (_doc + 1), _maxID) : NO_MORE_DOCS));
      }

      @Override
      public int advance(int id) throws IOException
      {
        if (_doc < id)
        {
          return (_doc = (id <= _maxID ? _dataCache.findValues(_bitset, id, _maxID) : NO_MORE_DOCS));
        }
        return nextDoc();
      }
  }

}
