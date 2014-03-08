package org.calrissian.hadoop.memcached;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.TapClient;
import net.spy.memcached.tapmessage.ResponseMessage;
import net.spy.memcached.tapmessage.TapStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A Hadoop {@link org.apache.hadoop.mapreduce.InputFormat} that will stream keys and values from a set of memcached
 * servers. It will initiate connections to each memcached server from different mappers so that the communications
 * can happen in parallel.
 */
public class MemcachedInputFormat extends InputFormat<Text,BytesWritable> {

    public static final String MEMCACHED_SERVERS_OPT = "couchbase.servers";

    /**
     * Returns one split for each memcached server. The length of the split is the expected number of keys
     * based on the stats.
     */
    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {

        String[] addresses = jobContext.getConfiguration().getStrings("couchbase.servers");

        List<InputSplit> splits = new ArrayList<InputSplit>();
        List<InetSocketAddress> socketAddresses= new ArrayList<InetSocketAddress>();
        for(final String address : addresses)
            socketAddresses.add(new InetSocketAddress(address, 11211));

        final MemcachedClient client = new MemcachedClient(socketAddresses);

        Map<SocketAddress, Map<String, String>> stats = client.getStats();
        for(final InetSocketAddress address : socketAddresses) {
            final Map<String,String> curStats = stats.get(address);
            splits.add(new InputSplit() {
                @Override
                public long getLength() throws IOException, InterruptedException {
                    return Long.parseLong(curStats.get("total_items"));
                }

                @Override
                public String[] getLocations() throws IOException, InterruptedException {
                    return new String[] { address.getHostName() };
                }
            });
        }
        return splits;
    }

    /**
     * Creates the record writer that will initiate a connection to the expected memcached server and return
     * each tapped key/value from the server.
     */
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        return new RecordReader<Text, BytesWritable>() {

            TapClient client;
            ResponseMessage currentMessage;
            TapStream tapStream;
            @Override
            public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                client = new TapClient(new InetSocketAddress(inputSplit.getLocations()[0], 11211));
                try {
                    tapStream = client.tapDump("tapDump_" + inputSplit.getLocations()[0]);
                } catch (ConfigurationException e) {
                    throw new IOException(e);
                };
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {

                if(tapStream.hasErrored())
                    throw new IOException("An error occurred in tap stream.");
                else if(tapStream.isCancelled())
                    throw new InterruptedException("The tap stream has been cancelled unexpectedly.");

                if(client.hasMoreMessages()) {
                    currentMessage = client.getNextMessage();
                    return true;
                }
                else {
                    currentMessage = null;
                    return false;
                }
            }

            @Override
            public Text getCurrentKey() throws IOException, InterruptedException {
                if(currentMessage != null)
                    return new Text(currentMessage.getKey());
                else
                    throw new InterruptedException("There are no more keys to return.");
            }

            @Override
            public BytesWritable getCurrentValue() throws IOException, InterruptedException {
                if(currentMessage != null)
                    return new BytesWritable(currentMessage.getValue());
                else
                    throw new InterruptedException("There are no more values to return");
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return client.getMessagesRead();
            }

            @Override
            public void close() throws IOException {
                client.shutdown();
            }
        };
    }

    /**
     * Sets the memcached servers on the job
     */
    public static void setMemcachedServersOpt(JobContext job, String[] servers) {
        job.getConfiguration().setStrings(MEMCACHED_SERVERS_OPT, servers);
    }
}
