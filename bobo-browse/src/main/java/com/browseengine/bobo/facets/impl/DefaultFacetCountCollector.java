package com.browseengine.bobo.facets.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import com.browseengine.bobo.facets.data.*;
import com.browseengine.bobo.util.*;
import org.apache.log4j.Logger;

import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.ComparatorFactory;
import com.browseengine.bobo.api.FacetIterator;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.api.FacetSpec.FacetSortSpec;
import com.browseengine.bobo.api.FieldValueAccessor;
import com.browseengine.bobo.facets.FacetCountCollector;
import com.browseengine.bobo.jmx.JMXUtil;
import com.browseengine.bobo.util.IntBoundedPriorityQueue.IntComparator;

public abstract class DefaultFacetCountCollector implements FacetCountCollector
{
  private static final Logger log = Logger.getLogger(DefaultFacetCountCollector.class.getName());
  protected final FacetSpec _ospec;
  public LazyBigIntArray _count;
  
  public int _countlength;
  protected FacetDataCache _dataCache;
  private final String _name;
  protected final BrowseSelection _sel;
//  protected final BigSegmentedArray _array;
  private int _docBase;
  protected final LinkedList<LazyBigIntArray> intarraylist = new LinkedList<LazyBigIntArray>();
  private Iterator _iterator;
  private boolean _closed = false;

  protected static MemoryManager<LazyBigIntArray> intarraymgr = new MemoryManager<LazyBigIntArray>(new MemoryManager.Initializer<LazyBigIntArray>()
  {
    public void init(LazyBigIntArray buf)
    {
      buf.fill(0);
    }

    public LazyBigIntArray newInstance(int size)
    {
      return new LazyBigIntArray(size);
    }

    public int size(LazyBigIntArray buf)
    {
      assert buf != null;
      return buf.size();
    }

  });
  
  static{
	  try{
		// register memory manager mbean
		MBeanServer mbeanServer = java.lang.management.ManagementFactory.getPlatformMBeanServer();
	    ObjectName mbeanName = new ObjectName(JMXUtil.JMX_DOMAIN,"name","DefaultFacetCountCollector-MemoryManager");
	    StandardMBean mbean = new StandardMBean(intarraymgr.getAdminMBean(), MemoryManagerAdminMBean.class);
	    mbeanServer.registerMBean(mbean, mbeanName);
	  }
	  catch(Exception e){
	    log.error(e.getMessage(),e);
	  }
  }

  public DefaultFacetCountCollector(String name,FacetDataCache dataCache,int docBase,
      BrowseSelection sel,FacetSpec ospec)
  {
    _sel = sel;
    _ospec = ospec;
    _name = name;
    _dataCache=dataCache;
    _countlength = _dataCache.getFreqSize();

      if (_dataCache.getFreqSize() <= 3096)
    {
      _count = new LazyBigIntArray(_countlength);
    } else
    {
      _count = intarraymgr.get(_countlength);
      intarraylist.add(_count);
    }
//    _array = _dataCache.orderArray;
    _docBase = docBase;
  }

  public String getName()
  {
    return _name;
  }

  abstract public void collect(int docid);

  abstract public void collectAll();

  public BrowseFacet getFacet(String value)
  {
    if (_closed)
    {
      throw new IllegalStateException("This instance of count collector for " + _name + " was already closed");
    }
    BrowseFacet facet = null;
    int index=_dataCache.getDocId(value);
    if (index >=0 ){
      facet = new BrowseFacet(_dataCache.getString(index),_count.get(index));
    }
    else{
      facet = new BrowseFacet(_dataCache.format(value),0);
    }
    return facet; 
  }

  public int getFacetHitsCount(Object value)
  {
    if (_closed)
    {
      throw new IllegalStateException("This instance of count collector for " + _name + " was already closed");
    }
    int index=_dataCache.getDocId(value);
    if (index >= 0)
    {
      return _count.get(index);
    }
    else{
      return 0;  
    }
  }

  public BigSegmentedArray getCountDistribution()
  {
    return _count;
  }
  
  public FacetDataCache getFacetDataCache(){
	  return _dataCache;
  }
  
  public static List<BrowseFacet> getFacets(FacetSpec ospec, BigSegmentedArray count, int countlength, final FacetDataCache facetDataCache){
	  if (ospec!=null)
	    {
	      int minCount=ospec.getMinHitCount();
	      int max=ospec.getMaxCount();
	      if (max <= 0) max=countlength;

	      List<BrowseFacet> facetColl;
	      FacetSortSpec sortspec = ospec.getOrderBy();
	      if (sortspec == FacetSortSpec.OrderValueAsc)
	      {
	        facetColl=new ArrayList<BrowseFacet>(max);
	        for (int i = 1; i < countlength;++i) // exclude zero
	        {
	          int hits=count.get(i);
	          if (hits>=minCount)
	          {
	            BrowseFacet facet=new BrowseFacet(facetDataCache.getString(i),hits);
	            facetColl.add(facet);
	          }
	          if (facetColl.size()>=max) break;
	        }
	      }
	      else //if (sortspec == FacetSortSpec.OrderHitsDesc)
	      {
	        ComparatorFactory comparatorFactory;
	        if (sortspec == FacetSortSpec.OrderHitsDesc){
	          comparatorFactory = new FacetHitcountComparatorFactory();
	        }
	        else{
	          comparatorFactory = ospec.getCustomComparatorFactory();
	        }

	        if (comparatorFactory == null){
	          throw new IllegalArgumentException("facet comparator factory not specified");
	        }

	        final IntComparator comparator = comparatorFactory.newComparator(new FieldValueAccessor(){

	          public String getFormatedValue(int index) {
	            return facetDataCache.getString(index);
	          }

	          public Object getRawValue(int index) {
	            return facetDataCache.getRawValue(index);
	          }

	        }, count);
	        facetColl=new LinkedList<BrowseFacet>();
	        final int forbidden = -1;
	        IntBoundedPriorityQueue pq=new IntBoundedPriorityQueue(comparator,max, forbidden);

	        for (int i=1;i<countlength;++i)
	        {
	          int hits=count.get(i);
	          if (hits>=minCount)
	          {
	            pq.offer(i);
	          }
	        }

	        int val;
	        while((val = pq.pollInt()) != forbidden)
	        {
	          BrowseFacet facet=new BrowseFacet(facetDataCache.getString(val),count.get(val));
	          ((LinkedList<BrowseFacet>)facetColl).addFirst(facet);
	        }
	      }
	      return facetColl;
	    }
	    else
	    {
	      return FacetCountCollector.EMPTY_FACET_LIST;
	    }
  }

    public static List<BrowseFacet> getFacets(FacetSpec ospec,BigSegmentedArray count, int countlength, final TermValueList<?> valList){
        if (ospec!=null)
        {
            int minCount=ospec.getMinHitCount();
            int max=ospec.getMaxCount();
            if (max <= 0) max=countlength;

            List<BrowseFacet> facetColl;
            FacetSortSpec sortspec = ospec.getOrderBy();
            if (sortspec == FacetSortSpec.OrderValueAsc)
            {
                facetColl=new ArrayList<BrowseFacet>(max);
                for (int i = 1; i < countlength;++i) // exclude zero
                {
                    int hits=count.get(i);
                    if (hits>=minCount)
                    {
                        BrowseFacet facet=new BrowseFacet(valList.get(i),hits);
                        facetColl.add(facet);
                    }
                    if (facetColl.size()>=max) break;
                }
            }
            else //if (sortspec == FacetSortSpec.OrderHitsDesc)
            {
                ComparatorFactory comparatorFactory;
                if (sortspec == FacetSortSpec.OrderHitsDesc){
                    comparatorFactory = new FacetHitcountComparatorFactory();
                }
                else{
                    comparatorFactory = ospec.getCustomComparatorFactory();
                }

                if (comparatorFactory == null){
                    throw new IllegalArgumentException("facet comparator factory not specified");
                }

                final IntComparator comparator = comparatorFactory.newComparator(new FieldValueAccessor(){

                    public String getFormatedValue(int index) {
                        return valList.get(index);
                    }

                    public Object getRawValue(int index) {
                        return valList.getRawValue(index);
                    }

                }, count);
                facetColl=new LinkedList<BrowseFacet>();
                final int forbidden = -1;
                IntBoundedPriorityQueue pq=new IntBoundedPriorityQueue(comparator,max, forbidden);

                for (int i=1;i<countlength;++i)
                {
                    int hits=count.get(i);
                    if (hits>=minCount)
                    {
                        pq.offer(i);
                    }
                }

                int val;
                while((val = pq.pollInt()) != forbidden)
                {
                    BrowseFacet facet=new BrowseFacet(valList.get(val),count.get(val));
                    ((LinkedList<BrowseFacet>)facetColl).addFirst(facet);
                }
            }
            return facetColl;
        }
        else
        {
            return FacetCountCollector.EMPTY_FACET_LIST;
        }
    }


    public List<BrowseFacet> getFacets() {
    if (_closed)
    {
      throw new IllegalStateException("This instance of count collector for " + _name + " was already closed");
    }
    
    return getFacets(_ospec,_count, _countlength, _dataCache);
    
  }

  @Override
  public void close()
  {
    if (_closed)
    {
      log.warn("This instance of count collector for '" + _name + "' was already closed. This operation is no-op.");
      return;
    }
    _closed = true;
    while(!intarraylist.isEmpty())
    {
      intarraymgr.release(intarraylist.poll());
    }
  }

  /**
   * This function returns an Iterator to visit the facets in value order
   * @return	The Iterator to iterate over the facets in value order
   */
  public FacetIterator iterator()
  {
    if (_closed)
    {
      throw new IllegalStateException("This instance of count collector for '" + _name + "' was already closed");
    }
    if (_dataCache.getValArray().getType().equals(Integer.class))
    {
      return new DefaultIntFacetIterator((TermIntList) _dataCache.getValArray(), _count, _countlength, false);
    } else if (_dataCache.getValArray().getType().equals(Long.class))
    {
      return new DefaultLongFacetIterator((TermLongList) _dataCache.getValArray(), _count, _countlength, false);
    } else if (_dataCache.getValArray().getType().equals(Short.class))
    {
      return new DefaultShortFacetIterator((TermShortList) _dataCache.getValArray(), _count, _countlength, false);
    } else if (_dataCache.getValArray().getType().equals(Float.class))
    {
      return new DefaultFloatFacetIterator((TermFloatList) _dataCache.getValArray(), _count, _countlength, false);
    } else if (_dataCache.getValArray().getType().equals(Double.class))
    {
      return new DefaultDoubleFacetIterator((TermDoubleList) _dataCache.getValArray(), _count, _countlength, false);
    } else
    return new DefaultFacetIterator(_dataCache.getValArray(), _count, _countlength, false);
  }
}
