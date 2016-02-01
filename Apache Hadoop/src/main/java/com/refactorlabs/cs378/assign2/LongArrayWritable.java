package com.refactorlabs.cs378.assign2;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class LongArrayWritable extends ArrayWritable implements WritableComparable {

    // call super class
    public LongArrayWritable() {
        super(LongWritable.class);
    }

    // get the values from the writable
    public long[] getValues() {
        Writable[] values = get();
        // create a long array to return
        long[] result = new long[values.length];
        // write the values from the writable to our long array
        for (int index = 0; index < result.length; index++) {
            result[index] = ((LongWritable) values[index]).get();
        }
        return result;
    }

    // from a given long array, write to a writable array
    public void setValues(long[] values) {
        Writable[] result = new LongWritable[values.length];
        // update the LongArrayWritable
        for (int index = 0; index < values.length; index++) {
            result[index] = new LongWritable(values[index]);
        }
        set(result);
    }

    @Override
    // from Java interface comparable
    public int compareTo(Object arg0) {
        // TODO Auto-generated method stub
        if (this.equals(arg0)) {
            return 0;
        } else {
            return 1;
        }
    }
}