package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 */

public class KMeansSingleIteration {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: hadoop jar kmeans.jar org.example.KMeansSingleIterationDriver <pointsPath> <centroidsPath> <outPath> [numReducers]");
            System.exit(1);
        }

        String pointsPath = args[0];
        String centroidsPath = args[1];
        String outPath = args[2];
        int numReducers = (args.length >= 4) ? Integer.parseInt(args[3]) : 1;

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "KMeans Single Iteration");

        job.setJarByClass(KMeansSingleIteration.class);

        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        job.setNumReduceTasks(numReducers);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // points input
        FileInputFormat.addInputPath(job, new Path(pointsPath));
        // output directory (must not exist)
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        // distribute centroids to all mappers
        job.addCacheFile(new java.net.URI(centroidsPath + "#centroids"));

        System.exit(job.waitForCompletion(true) ? 0 : 2);
    }
}
