package com.browseengine.bobo.util;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

/**
 * Recreates a int[][] using a single contiguous block of NIO heap. Does so using row-major logic. When a row fills
 * up we attempt to either shuffle data into neighboring rows or expand the entire matrix.
 *
 * This process can obviously be rather wasteful, so there is compaction that can be done after loading the array
 * initially.
 */
public class NioMatrix {
    private IntBuffer buffer;
    private IntBuffer offsets;
    private IntBuffer lengths;
    private int numRows;
    private int totalSize;

    private static final int DEFAULT_COLUMN_WIDTH = 8;

    public NioMatrix(int numRows) {
        offsets = ByteBuffer.allocateDirect(4 * numRows).asIntBuffer();
        for(int i = 0; i < numRows; i++) {
            offsets.put(i, DEFAULT_COLUMN_WIDTH * i);
        }

        lengths = ByteBuffer.allocateDirect(4 * numRows).asIntBuffer();

        totalSize = (numRows * DEFAULT_COLUMN_WIDTH);
        buffer = ByteBuffer.allocateDirect(4 * totalSize).asIntBuffer();
        this.numRows = numRows;
    }

    private final int getCapacity(int row) {
        int offset = offsets.get(row);
        return (row == numRows - 1) ? (totalSize - offset) : (offsets.get(row+1) - offset);
    }

    private final boolean hasRoom(int row, float K) {
        if(row < 0 || row >= numRows)
            return false;

        int capacity = getCapacity(row);
        int columnLength = getColumnLength(row);
        float fraction = ((float) columnLength) / capacity;
        return fraction <= K && (capacity - columnLength >= 1);
    }

    private final void shuffeLower(int row) {
        // Take half the available space
        int available = getCapacity(row - 1) - getColumnLength(row - 1);
        int toTake = Math.max(1, available / 2);

        int offset = offsets.get(row);
        int newOffset = offset - toTake;
        int columnLength = getColumnLength(row);
        for(int i = 0; i < columnLength; i++) {
            buffer.put(newOffset + i, buffer.get(offset + i));
        }
        offsets.put(row, newOffset);
    }

    private final void shuffleUpper(int row) {
        // Take half the available space
        int upperColumnLength = getColumnLength(row + 1);
        int available = getCapacity(row + 1) - upperColumnLength;
        int toTake = Math.max(1, available / 2);

        int upperOffset = offsets.get(row + 1);
        int newUpperOffset = upperOffset + toTake;

        for(int i = upperColumnLength - 1; i >= 0; i--) {
            int upperPointer = newUpperOffset + i;
            int lowerPointer = upperOffset + i;

            buffer.put(upperPointer, buffer.get(lowerPointer));
        }

        offsets.put(row + 1, newUpperOffset);
    }

    protected void increaseSize() {
        // Double the length of every single column
        IntBuffer newBuffer = ByteBuffer.allocateDirect(4 * (2 * totalSize + 5 * numRows)).asIntBuffer();
        IntBuffer newOffsets = ByteBuffer.allocateDirect(4 * numRows).asIntBuffer();
        int totalOffset = 0;

        for(int i = 0; i < numRows; i++) {
            newOffsets.put(i, totalOffset);
            int rowOffset = offsets.get(i);

            for(int j = 0; j < getColumnLength(i); j++) {
                int v = buffer.get(rowOffset + j);
                newBuffer.put(totalOffset + j, v);
            }

            totalOffset += (2 * getCapacity(i) + 5);
        }

        buffer = newBuffer;
        offsets = newOffsets;
        totalSize = totalOffset;
    }

    public void add(int row, int value) {
        if(row <= numRows) {
            // we're not out of bounds! perfect!
            int offset = offsets.get(row);
            int columnLength = getColumnLength(row);
            int columnCapacity = getCapacity(row);

            if(columnLength < columnCapacity) {
                buffer.put(offset + columnLength, value);
                lengths.put(row, lengths.get(row) + 1);
            } else {
                // The column is full. Can we squeeze our neighbors?
                boolean lowerHasRoom = hasRoom(row - 1, 0.85f);
                boolean upperHasRoom = hasRoom(row + 1, 0.85f);

                if(lowerHasRoom) {
                    shuffeLower(row);
                } else if (upperHasRoom) {
                    shuffleUpper(row);
                } else {
                    increaseSize();
                }

                add(row, value);
            }
        } else {
            throw new IllegalArgumentException("Row size must be selected statically, sorry!");
        }
    }

    /**
     * Optimize the buffer to minimize memory usage.
     */
    public final void compact() {
       int size = 0;
       for(int i = 0; i < numRows; i++) {
           size += getColumnLength(i);
       }

       IntBuffer newBuffer = ByteBuffer.allocateDirect(4 * size).asIntBuffer();
       IntBuffer newOffsets = ByteBuffer.allocateDirect(4 * numRows).asIntBuffer();

       int pointer = 0;
       for(int row = 0; row < numRows; row++) {
         newOffsets.put(row, pointer);
         int rowOffset = offsets.get(row);
         int columnLength = getColumnLength(row);
         for(int col = 0; col < columnLength; col++) {
           newBuffer.put(pointer++, buffer.get(rowOffset + col));
         }
       }

       buffer = newBuffer;
       offsets = newOffsets;
    }

    public int get(int row, int col) {
        int offset = offsets.get(row);
        return buffer.get(offset + col);
    }

    public int getRowLength() {
        return numRows;
    }

    public int getColumnLength(int row) {
        return lengths.get(row);
    }

    public int binarySearch(int row, int value) {
        return binarySearch(row, 0, lengths.get(row), value);
    }

    public int binarySearch(int row, int colStart, int colEnd, int value) {
      int lo = offsets.get(row) + colStart;
      int hi = offsets.get(row) + colEnd;
      while(lo < hi) {
        int mid = lo + (hi - lo) / 2;
        if(buffer.get(mid) < value) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }

      if(lo == hi && lo < offsets.get(row) + colEnd && buffer.get(lo) == value)
        return lo - offsets.get(row);
      else
        return -(lo - offsets.get(row)) - 1;
    }
}
