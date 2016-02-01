package com.refactorlabs.cs378.assign4;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Jong Hoon Lim on 9/28/15.
 */
public class WordStatisticsWritable implements Writable {

    // primitives for WordStatisticsWritable to hold
    private long paragraph;
    private long count;
    private long squareSum;
    private double mean;
    private double variance;

    // empty constructor
    public WordStatisticsWritable() {

    }

    // constructor given all values (for Aggregator)
    public WordStatisticsWritable(long paragraph, long count, long squareSum, double mean, double variance) {
        this.paragraph = paragraph;
        this.count = count;
        this.squareSum = squareSum;
        this.mean = mean;
        this.variance = variance;
    }

    // initially update paragraph, word count, square sum count for the mapper in WordStatistics
    public void initialCount(long paragraph, long count, long squareSum) {
        this.paragraph = paragraph;
        this.count = count;
        this.squareSum = squareSum;
    }

    // reset the WordStatisticsWritable
    public void reset() {
        this.paragraph = 0;
        this.count = 0;
        this.squareSum = 0;
        this.mean = 0.0;
        this.variance = 0.0;
    }

    // update the WordStatisticsWritable with new values
    // update counts for the reducer
    public void updateWritable(WordStatisticsWritable otherWritable) {
        // update values
        this.paragraph += otherWritable.paragraph;
        this.count += otherWritable.count;
        this.squareSum += otherWritable.squareSum;

        // compute the mean and variance
        this.mean = (1.0 * count) / paragraph;
        this.variance = (1.0 * this.squareSum / this.paragraph) - (1.0 * this.mean * this.mean);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.paragraph);
        out.writeLong(this.count);
        out.writeLong(this.squareSum);
        out.writeDouble(this.mean);
        out.writeDouble(this.variance);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.paragraph = in.readLong();
        this.count = in.readLong();
        this.squareSum = in.readLong();
    }

    public String toString() {
        // Use StringBuilder to easily add elements
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(this.paragraph);
        strBuilder.append(',');
        strBuilder.append(this.count);
        strBuilder.append(',');
        strBuilder.append(this.squareSum);
        strBuilder.append(',');
        strBuilder.append(this.mean);
        strBuilder.append(',');
        strBuilder.append(this.variance);

        return strBuilder.toString();
    }
}
