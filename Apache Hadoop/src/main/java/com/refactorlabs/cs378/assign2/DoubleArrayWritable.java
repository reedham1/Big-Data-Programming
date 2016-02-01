package com.refactorlabs.cs378.assign2;

import java.io.DataOutput;
import java.io.IOException;
import java.lang.Override;
import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class DoubleArrayWritable extends ArrayWritable implements WritableComparable {

    public DoubleArrayWritable() {
        super(DoubleWritable.class);
    }

    public double[] getValues() {
        Writable[] values = get();
        double[] result = new double[values.length];

        for (int index = 0; index < result.length; index++) {
            result[index] = ((DoubleWritable)values[index]).get();
        }
        return result;
    }

    public void setValues(double[] values) {
        Writable[] result = new DoubleWritable[values.length];

        for (int index = 0; index < values.length; index++) {
            result[index] = new DoubleWritable(values[index]);
        }
        set(result);
    }

    @Override
    // Override toString()
    public String toString() {
        double[] values = getValues();
        return Arrays.toString(values);
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