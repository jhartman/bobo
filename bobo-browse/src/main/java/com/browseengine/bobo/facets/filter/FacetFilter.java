package com.browseengine.bobo.facets.filter;

import java.io.IOException;

import com.browseengine.bobo.facets.data.FacetDataCache;
import org.apache.lucene.search.DocIdSetIterator;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.docidset.EmptyDocIdSet;
import com.browseengine.bobo.docidset.RandomAccessDocIdSet;
import com.browseengine.bobo.facets.FacetHandler;
//import com.browseengine.bobo.facets.data.FacetDataCache;


public class FacetFilter extends RandomAccessFilter 
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	

  private final FacetHandler<FacetDataCache> _facetHandler;
	protected final String _value;
	
	public FacetFilter(FacetHandler<FacetDataCache> facetHandler,String value)
	{
		_facetHandler = facetHandler;
		_value=value;
	}
	
  public double getFacetSelectivity(BoboIndexReader reader)
  {
    double selectivity = 0;
    FacetDataCache dataCache = _facetHandler.getFacetData(reader);
    int idx = dataCache.getDocId(_value);
    if(idx<0)
    {
      return 0.0;
    }
    int freq =dataCache.getFreq(idx);
    int total = reader.maxDoc();
    selectivity = (double)freq/(double)total;
    return selectivity;
  }
  
	public static class FacetDocIdSetIterator extends DocIdSetIterator
	{
		protected int _doc;
		protected final int _index;
		protected final int _maxID;
        protected final FacetDataCache dataCache;

		public FacetDocIdSetIterator(FacetDataCache dataCache,int index)
		{
			_index=index;
			_doc=Math.max(-1,dataCache.getMinId(_index)-1);
			_maxID = dataCache.getMaxId(_index);
            this.dataCache = dataCache;
		}
		
		@Override
		final public int docID() {
			return _doc;
		}

		@Override
		public int nextDoc() throws IOException
		{
          return (_doc = (_doc < _maxID ? dataCache.findValue(_index, (_doc + 1), _maxID) : NO_MORE_DOCS));
		}

		@Override
		public int advance(int id) throws IOException
		{
          if (_doc < id)
          {
            return (_doc = (id <= _maxID ? dataCache.findValue(_index, id, _maxID) : NO_MORE_DOCS));
          }
          return nextDoc();
        }

	}

	@Override
	public RandomAccessDocIdSet getRandomAccessDocIdSet(BoboIndexReader reader) throws IOException 
	{
		FacetDataCache dataCache = _facetHandler.getFacetData(reader);
		int index = dataCache.getDocId(_value);
		if (index < 0)
		{
		  return EmptyDocIdSet.getInstance();
		}
		else
		{
			return new FacetDataRandomAccessDocIdSet(dataCache, index);
		}
	}

	public static class FacetDataRandomAccessDocIdSet extends RandomAccessDocIdSet{

		private final FacetDataCache _dataCache;
	    private final int _index;
	    
	    FacetDataRandomAccessDocIdSet(FacetDataCache dataCache,int index){
	    	_dataCache = dataCache;
	    	_index = index;
	    }
		@Override
		public boolean get(int docId) {
            return _dataCache.getOrderArrayValue(docId) == _index;
		}

		@Override
		public DocIdSetIterator iterator() throws IOException {
			return new FacetDocIdSetIterator(_dataCache,_index);
		}
		
	}
}
