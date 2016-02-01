package com.refactorlabs.cs378.assign3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Created by Jong Hoon Lim on 9/22/15.
 */
// Class to represent the Verses
public class StringWritable implements Writable {

    private String str;

    // Empty Constructor
    public StringWritable() {}

    // create a StringWritable with the given String
    public StringWritable(String inputString) {
        this.str = inputString;
    }

    public void readFields(DataInput in) throws IOException {
        str = in.readUTF();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(str);
    }

    public String toString() {
        return str;
    }
}
