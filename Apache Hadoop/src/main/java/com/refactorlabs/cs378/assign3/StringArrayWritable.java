package com.refactorlabs.cs378.assign3;

import java.util.Arrays;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

/**
 * Created by Jong Hoon Lim on 9/22/15.
 */
// ArrayWritable for the verses
public class StringArrayWritable extends ArrayWritable {

    public StringArrayWritable() {
        super(StringWritable.class);
    }


    // Constructor for given StringWritable Array
    public StringArrayWritable(StringWritable[] strWritable) {
        super(StringWritable.class);
        set(strWritable);
    }

    // Constructor for given ArrayList of StringWritables
    public StringArrayWritable(ArrayList<StringWritable> arrList) {
        super(StringWritable.class);
        // create an array same size as given ArrayList
        StringWritable[] strWritableArray = new StringWritable[arrList.size()];

        // copy over to array
        for (int index = 0; index < strWritableArray.length; index++) {
            strWritableArray[index] = (StringWritable)arrList.get(index);
        }

        set(strWritableArray);
    }

    // print String verses
    public String toString() {
        Writable[] values = get();
        return StringUtils.join(values, ",");
    }
}