package com.browseengine.bobo.util;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;

public class SimpleIntMatrix {
    private IntArrayList[] buffer;

    public SimpleIntMatrix(int numRows) {
        numRows = numRows;
        buffer = new IntArrayList[numRows];
        for(int row = 0; row < buffer.length; row++) {
            buffer[row] = new IntArrayList();
        }
    }

    public void add(int row, int value) {
        if(row <= buffer.length) {
            buffer[row].add(value);
        } else {
            throw new IllegalArgumentException("Row size must be selected statically, sorry!");
        }
    }

    public int get(int row, int col) {
        return buffer[row].get(col);
    }

    public int getRowLength() {
        return buffer.length;
    }

    public int getColumnLength(int row) {
        return buffer[row].size();
    }

    public int binarySearch(int row, int value) {
        return binarySearch(row, 0, buffer[row].size(), value);
    }

    public int binarySearch(int row, int colStart, int colEnd, int value) {
        return Arrays.binarySearch(buffer[row].elements(), colStart, colEnd, value);
    }
}
