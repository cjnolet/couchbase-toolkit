package org.calrissian.hadoop.memcached;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CouchbaseInputSplit extends InputSplit implements Writable {

    short[] locations;

    public CouchbaseInputSplit() {

    }

    public CouchbaseInputSplit(short[] locations) {
        this.locations = locations;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return locations.length;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        String[] sLocs = new String[locations.length];

        for (int i = 0; i < locations.length; i++) {
            sLocs[i] = Short.toString(locations[i]);
        }
        return sLocs;
    }

    public short[] getVBuckets() {
        return locations;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeShort(locations.length);
        for (int i = 0; i < locations.length; i++) {
            output.writeShort(locations[i]);
        }
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        int length = input.readShort();
        locations = new short[length];
        for (int i = 0; i < locations.length; i++) {
            locations[i] = input.readShort();
        }
    }
}
