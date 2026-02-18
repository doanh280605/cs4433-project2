package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

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
 * The code doesn't iterate until convergence; it executes KMeans once. The output
 * centroids are intended to be used as input for subsequent iterations.
 */
public class KMeansSingleIteration {

    public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final Map<Integer, double[]> centroids = new HashMap<>();
        private final IntWritable outKey = new IntWritable();
        private final Text outValue = new Text();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] files = context.getCacheFiles();
            if (files == null || files.length == 0) {
                throw new IOException("Centroid file not found in Distributed Cache");
            }

            // Because driver uses: job.addCacheFile(new URI(centroidsPath + "#centroids"));
            // Hadoop creates a local symlink named "centroids" in the task working dir.
            try (BufferedReader br = new BufferedReader(new FileReader("centroids"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    String[] tabParts = line.split("\\t");
                    if (tabParts.length != 2) throw new IOException("Centroid file format error: " + line);

                    int cid = Integer.parseInt(tabParts[0].trim());
                    String[] xy = tabParts[1].split(",");
                    if (xy.length != 2) throw new IOException("Centroid file format error: " + line);

                    double cx = Double.parseDouble(xy[0].trim());
                    double cy = Double.parseDouble(xy[1].trim());
                    centroids.put(cid, new double[]{cx, cy});
                }
            }

            if (centroids.isEmpty()) {
                throw new IOException("No centroids loaded (empty centroid file?)");
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String s = value.toString().trim();
            if (s.isEmpty()) return;

            String[] xy = s.split(",");
            if (xy.length != 2) return;

            double x = Double.parseDouble(xy[0].trim());
            double y = Double.parseDouble(xy[1].trim());

            int bestCid = -1;
            double bestDistance = Double.MAX_VALUE;

            for (Map.Entry<Integer, double[]> e : centroids.entrySet()) {
                double[] c = e.getValue();
                double dx = x - c[0];
                double dy = y - c[1];
                double dist = dx * dx + dy * dy; // squared distance

                if (dist < bestDistance) {
                    bestDistance = dist;
                    bestCid = e.getKey();
                }
            }

            outKey.set(bestCid);
            outValue.set(x + "," + y);
            context.write(outKey, outValue);
        }
    }

    public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private final Text outVal = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {

            double sumX = 0.0, sumY = 0.0;
            long count = 0;

            for (Text v : values) {
                String[] xy = v.toString().split(",");
                if (xy.length != 2) continue;
                sumX += Double.parseDouble(xy[0].trim());
                sumY += Double.parseDouble(xy[1].trim());
                count++;
            }

            if (count == 0) return;

            double cx = sumX / count;
            double cy = sumY / count;

            outVal.set(cx + "," + cy);
            ctx.write(key, outVal);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println(
                    "Usage: hadoop jar kmeans.jar org.example.KMeansSingleIteration <pointsPath> <centroidsPath> <outPath> [numReducers]"
            );
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

        FileInputFormat.addInputPath(job, new Path(pointsPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        // distribute centroids to all mappers (creates local symlink "centroids")
        job.addCacheFile(new URI(centroidsPath + "#centroids"));

        System.exit(job.waitForCompletion(true) ? 0 : 2);
    }
}
