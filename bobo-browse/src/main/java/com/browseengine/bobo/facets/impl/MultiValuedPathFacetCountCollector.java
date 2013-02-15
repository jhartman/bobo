package com.browseengine.bobo.facets.impl;

import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
//import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;

public class MultiValuedPathFacetCountCollector extends PathFacetCountCollector {

	public MultiValuedPathFacetCountCollector(String name, String sep,
			BrowseSelection sel, FacetSpec ospec, FacetDataCache dataCache) {
		super(name, sep, sel, ospec, dataCache);
	}

	@Override
    public final void collect(int docid) 
    {
      ((MultiValueFacetDataCache)_dataCache).countNoReturn(docid, _count);
    }

    @Override
    public final void collectAll()
    {
      _count = _dataCache.getFreqs();
    }
}
