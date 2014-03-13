package org.calrissian.hadoop.memcached;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.TapClient;
import net.spy.memcached.tapmessage.MessageBuilder;
import net.spy.memcached.tapmessage.ResponseMessage;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * Created by cjnolet on 3/12/14.
 */
public class driver {

    public static void main(String[] args) throws URISyntaxException, IOException, ConfigurationException {

        TapClient client = new TapClient(Arrays.asList(new URI[] { new URI("http://mediaserver:8091/pools")}), "default", "");
        CouchbaseClient client2 = new CouchbaseClient(Arrays.asList(new URI[] { new URI("http://mediaserver:8091/pools")}), "default", "");
        MessageBuilder messageBuilder = new MessageBuilder();
        messageBuilder.supportAck();
        messageBuilder.doDump();

        short[] vbuckets = new short[client2.getNumVBuckets()];
        for(int i = 0; i < vbuckets.length; i++) {
            vbuckets[i] = (short)i;
        }

        messageBuilder.specifyVbuckets(vbuckets);
        client.tapCustom(null, messageBuilder.getMessage());

        ResponseMessage message;

        while(client.hasMoreMessages()) {
            while((message = client.getNextMessage()) == null) {

                if(!client.hasMoreMessages())
                    break;
            }

            if(message != null) {
                System.out.println(message.getKey());
                System.out.println(new String(message.getValue()));
            }
        }

        System.out.println(1024 / 5);

        System.exit(0);
    }
}
