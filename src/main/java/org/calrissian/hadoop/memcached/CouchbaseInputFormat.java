package org.calrissian.hadoop.memcached;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.TapClient;
import net.spy.memcached.tapmessage.MessageBuilder;
import net.spy.memcached.tapmessage.ResponseMessage;
import net.spy.memcached.tapmessage.TapStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * An input format for loading keys and values from memcached instances backing Couchbase by using the TAP Dump
 * protocol. This input format is largely inspired by the Couchbase Sqoop Plugin but allows it to be wired up
 * in a generic mapreduce job- not tied to Sqoop. This input format allows the user to control the amount of
 * parallelism that is used to process the keys/values. Couchbase does not expose the locations of its vbuckets
 * so data locality is not possible.
 */
public class CouchbaseInputFormat extends InputFormat<Text, BytesWritable> {

    public static final String PARALLELISM_OPT = "couchbase.parallelism";
    public static final String BUCKET_OPT = "couchbase.bucket";
    public static final String PASSWORD_OPT = "couchbase.password";
    public static final String LOCATIONS_OPT = "couchbase.locations";

    /**
     * A split point essentially defines some chunk of vbuckets to load. The number of vbuckets assigned to each
     * split depends on
     */
    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {

        List<URI> locactions = new LinkedList<URI>();
        for(String location : getLocations(jobContext)) {
            try {
                locactions.add(new URI(location));
            } catch (URISyntaxException e) {
                throw new IOException("Invalid URI in location: " + location);
            }
        }
        CouchbaseClient client = new CouchbaseClient(locactions, getBucket(jobContext), getPassword(jobContext));
        int numVBuckets = client.getNumVBuckets();

        int itemsPerChunk = numVBuckets / getParallelism(jobContext);
        int extraItems = numVBuckets % getParallelism(jobContext);

        List<InputSplit> splits = new ArrayList<InputSplit>();

        int splitIndex = 0;
        short[] curSplit = nextEmptySplit(itemsPerChunk, extraItems);
        extraItems--;

        for (short i = 0; i < numVBuckets + 1; i++) {
            if (splitIndex == curSplit.length) {
                CouchbaseInputSplit split = new CouchbaseInputSplit(curSplit);
                splits.add(split);
                curSplit = nextEmptySplit(itemsPerChunk, extraItems);
                extraItems--;
                splitIndex = 0;
            }
            curSplit[splitIndex] = i;
            splitIndex++;
        }

        return splits;
    }

    private short[] nextEmptySplit(int itemsPerChunk, int extraItems) {
        if (extraItems > 0) {
            return new short[itemsPerChunk + 1];
        } else {
            return new short[itemsPerChunk];
        }
    }

    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new RecordReader<Text, BytesWritable>() {

            TapClient tapClient;
            TapStream stream;
            Text sharedKey = new Text();
            BytesWritable sharedValue;


            @Override
            public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                List<URI> uris = new LinkedList<URI>();
                for(String location : getLocations(taskAttemptContext)) {
                    try {
                        uris.add(new URI(location));
                    } catch (URISyntaxException e) {
                        throw new IOException("Location is not a valid URI: " + location);
                    }
                }
                tapClient = new TapClient(uris, getBucket(taskAttemptContext), getPassword(taskAttemptContext));
                MessageBuilder messageBuilder = new MessageBuilder();
                messageBuilder.specifyVbuckets(((CouchbaseInputSplit)inputSplit).getVBuckets());
                messageBuilder.doDump();
                messageBuilder.supportAck();

                try {
                    tapClient.tapCustom(null, messageBuilder.getMessage());
                } catch (ConfigurationException e) {
                    throw new IOException(e);
                }
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {

                if(stream.isCancelled())
                    throw new IOException("The stream has been cancelled");
                else if(stream.hasErrored())
                    throw new IOException("The stream has errored unexpectedly");

                ResponseMessage message;
                while ((message = tapClient.getNextMessage()) == null) {
                    if (!tapClient.hasMoreMessages()) {
                        return false;
                    }
                }

                sharedKey.set(message.getKey());
                sharedValue = new BytesWritable(message.getValue());

                return true;
            }

            @Override
            public Text getCurrentKey() throws IOException, InterruptedException {
                return sharedKey;
            }

            @Override
            public BytesWritable getCurrentValue() throws IOException, InterruptedException {
                return sharedValue;
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                // Since we don't know how many messages are coming progress doesn't
                // make much sense so either we're all the way done or not done at all.
                if (tapClient.hasMoreMessages()) {
                    return 0;
                }
                return 1;            }

            @Override
            public void close() throws IOException {

            }
        };
    }

    public static String[] getLocations(JobContext job) {
        return job.getConfiguration().getStrings(LOCATIONS_OPT, new String[]{});
    }

    public static int getParallelism(JobContext job) {
        return job.getConfiguration().getInt(PARALLELISM_OPT, 1);
    }

    public static String getBucket(JobContext job) {
        return job.getConfiguration().get(BUCKET_OPT, "default");
    }

    public static String getPassword(JobContext job) {
        return job.getConfiguration().get(PASSWORD_OPT, "");
    }

    /**
     * Sets the number of parallel mappers to use for funneling in the vbuckets.
     */
    public static void setParallelismOpt(JobContext job, int parallelism) {
        job.getConfiguration().setInt(PARALLELISM_OPT, parallelism);
    }

    /**
     * Set the bucket to dump
     */
    public static void setBucketOpt(JobContext job, String bucketName) {
        job.getConfiguration().set(BUCKET_OPT, bucketName);
    }

    /**
     * Set the password of the bucket to dump
     */
    public static void setPasswordOpt(JobContext job, String password) {
        job.getConfiguration().set(PASSWORD_OPT, password);
    }

    /**
     * Set the locations of the couchbase nodes. These should be the full
     * URIs (i.e. http://localhost:8091/pools)
     */
    public static void setLocationsOpt(JobContext job, String[] locations) {
        job.getConfiguration().setStrings(LOCATIONS_OPT, locations);
    }
}
