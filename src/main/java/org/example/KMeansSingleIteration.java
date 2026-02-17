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
 * This implementation performs exactly one iteration of the KMeans clustering algorithm
 * using one MapReduce job
 *
 * Input:
 * - a fixed dataset of 2D points stored in HDFS
 * - an initial centroid file having K cluster centers also stored in HDFS
 *
 * Mapper:
 * - Load the current centroids using Hadoop's Distributed Cache
 * - For each input point, computes the Euclidean distance to every centroid
 * - Assigns the point to the nearest centroid
 * - Emit clusterId, point pairs
 *
 * Reducer:
 * - Receives all points assigned to the same clusterId
 * - Computes the new centroid by averaging the x y coordinates of the points
 * - Outputs clusterId, new centroid coordinates
 *
 * Output:
 * - A new centroid file representing updated cluster centers after 1 iteration
 *
 * The code doesn't itearate until convergence, it executes the KMeans once. The output
 * centroids are intended to be used as input for subsequent iterations in multi-iteration
 * or early-stopping version of the algorithm
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
