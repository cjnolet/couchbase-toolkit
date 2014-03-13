package org.calrissian.hadoop.memcached;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class CouchbaseExampleMR extends Configured implements Tool {

    public static void main(String args[]) throws Exception {

        if(args.length == 0) {
            System.out.println("Arguments: <couchbase uri> [<couchbase uri>...]");
            System.exit(1);
        }

        System.exit(ToolRunner.run(new CouchbaseExampleMR(), args));
    }
    @Override
    public int run(String[] strings) throws Exception {

        Job job = new Job(getConf(), "Example Couchbase Job");

        job.setJarByClass(getClass());
        job.setInputFormatClass(CouchbaseInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        job.setNumReduceTasks(0);

        job.setMapperClass(ExampleMapper.class);

        CouchbaseInputFormat.setLocationsOpt(job, strings);
        CouchbaseInputFormat.setParallelismOpt(job, 2);

        return job.waitForCompletion(true) == false ? 1 : 0;
    }

    private static class ExampleMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {

        private int numProcessed = 0;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            numProcessed++;
            context.getCounter("STATS", "keys").increment(1);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println("Processed " + numProcessed + " keys.");
        }
    }
}
