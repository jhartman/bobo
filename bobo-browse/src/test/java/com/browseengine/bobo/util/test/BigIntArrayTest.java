/**
 * 
 */
package com.browseengine.bobo.util.test;

import com.browseengine.bobo.util.BigIntArray;

import com.browseengine.bobo.util.BufferedIntBuffer;
import junit.framework.TestCase;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class BigIntArrayTest extends TestCase
{
  public static void testBigIntArray()
  {
    int count = 5000000;
    BigIntArray test = new BigIntArray(count);
    int[] test2 = new int[count];
    IntBuffer test3 = ByteBuffer.allocateDirect(4 * count).asIntBuffer();
    for (int i = 0; i < count; i++)
    {
      test.add(i, i);
      test2[i]=i;
      test3.put(i, i);
    }

    BufferedIntBuffer test4 = new BufferedIntBuffer(test3);
    for (int i = 0; i< count; i++)
    {
      assertEquals(0, test.get(0));
    }

    for(int run = 0; run < 100; run++) {
        int k = 0;
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++)
        {
          k += test.get(i);
        }
        long end = System.currentTimeMillis();
        System.out.println("Big array took: "+(end-start));

        start = System.currentTimeMillis();
        for (int i = 0; i < count; i++)
        {
          k += test2[i];
        }
        end=System.currentTimeMillis();
        System.out.println("int[] took: "+(end-start));

        start = System.currentTimeMillis();
        for(int i = 0; i < count; i++)
        {
          k += test3.get(i);
        }
        end = System.currentTimeMillis();
        System.out.println("Int buffer took: " + (end - start));

        start = System.currentTimeMillis();
        for(int i = 0; i < count; i++)
        {
            k += test4.get(i);
        }
        end = System.currentTimeMillis();
        System.out.println("Buffered Int buffer took: " + (end - start));

    }
  }
}
