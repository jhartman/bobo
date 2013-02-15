package com.browseengine.bobo.test;

import com.browseengine.bobo.util.NioMatrix;
import com.browseengine.bobo.util.SimpleIntMatrix;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.Random;

public class TestNioMatrix extends TestCase {
  @Test
  public void testMatrix() {
      Random random = new Random();
      for(int numRows = 2; numRows <= 100; numRows++) {
          NioMatrix nioMatrix = new NioMatrix(numRows);
          SimpleIntMatrix intMatrix = new SimpleIntMatrix(numRows);

          for(int i = 0; i < numRows * 20; i++) {
              int row = random.nextInt(numRows);
              int value = random.nextInt(100000);
              nioMatrix.add(row, value);
              intMatrix.add(row, value);
              int columnLength = intMatrix.getColumnLength(row);

//              System.out.println(i);


//              assertEquals(intMatrix.get(row, columnLength - 1), nioMatrix.get(row, columnLength - 1));
          }

          for(int r = 0; r < numRows; r++) {
              for(int c = 0; c < intMatrix.getColumnLength(r); c++) {
                  int realValue = intMatrix.get(r,c);
                  int testValue = nioMatrix.get(r,c);
//                  System.out.println("R: " + r + " C: " + c);
                  assertEquals(realValue, testValue);
              }
          }
      }
  }
}
