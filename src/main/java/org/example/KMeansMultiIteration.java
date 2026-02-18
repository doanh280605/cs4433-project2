package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.*;

/**
 * This implementation performs a basic multi-iteration KMeans clustering algorithm
 * using multiple MapReduce jobs, stop after a fixed number of iteration
 *
 * Input:
 * - Fixed dataset of 2D points stored in HDFS
 * - Initial centroid file with K cluster centers stored in HDFS
 * - Defined max number of iterations R
 *
 * Process:
 * - The main program controls the iteration loop
 * - Each iteration launches one MapReduce job
 * - The output centroids from iteration i are used as input centroids for the next iteration
 * - The algorithm run exactly R iterations
 *
 * Mapper:
 * - Loads the current iteration's centroids using Hadoop's Distributed Cache
 * - For each input point, computes the Euclidean distance to all centroids
 * - Assigns the point to the nearest centroid
 * - Emit clusterId, point pairs
 *
 * Reducer:
 * - Receives all points assigned to the same clusterId
 * - Computes the new centroid by averaging the x y coordinates
 * - Outputs clusterId and new centroid coordinates
 *
 * Output:
 * - For each iteration, a new centroid file in HDFS
 * - After R iterations, the final centroid file represents the result of the algorithm
 *
 * The code doesn't check for convergence. Termination is controlled strictly by the
 * number of iterations. This implementation serves as the foundation for early stopping
 * and optimized versions of KMeans
 */

public class KMeansMultiIteration {
    public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final Map<Integer, double[]> centroids = new HashMap<>();
        private final IntWritable outKey = new IntWritable();
        private final Text outValue = new Text();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] files = context.getCacheFiles();
            if (files == null || files.length == 0) {
                throw new IOException("Centroid file not found");
            }

            File centroidFile = new File(files[0].getPath());

            try (BufferedReader br = new BufferedReader(new FileReader(centroidFile))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    String[] tabParts = line.split("\\t");
                    if (tabParts.length != 2) {
                        throw new IOException("Centroid file format error");
                    }

                    int cid = Integer.parseInt(tabParts[0].trim());
                    String[] xy = tabParts[1].split(",");
                    if (xy.length != 2) {
                        throw new IOException("Centroid file format error");
                    }

                    double cx = Double.parseDouble(xy[0].trim());
                    double cy = Double.parseDouble(xy[1].trim());
                    centroids.put(cid, new double[]{cx, cy});
                }
            }

            if (centroids.isEmpty()) {
                throw new IOException("No centroids loaded");
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString().trim();
            if (s.isEmpty()) return;

            String[] xy = s.split(",");
            if (xy.length != 2) return;

            double x = Double.parseDouble(xy[0]);
            double y = Double.parseDouble(xy[1]);

            int bestCid = -1;
            double bestDistance = Double.MAX_VALUE;

            for (Map.Entry<Integer, double[]> e : centroids.entrySet()) {
                int cid = e.getKey();
                double[] c = e.getValue();

                double dx = x - c[0];
                double dy = y - c[1];
                double distance = dx * dx + dy * dy;

                // optional deterministic tie break
                if (distance < bestDistance || (distance == bestDistance && cid < bestCid)) {
                    bestDistance = distance;
                    bestCid = cid;
                }
            }
            outKey.set(bestCid);
            outValue.set(x + "," + y);
            context.write(outKey, outValue);
        }
    }

    public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private final Text outValue = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sumX = 0.0, sumY = 0.0;
            long count = 0;

            for (Text value : values) {
                String[] xy = value.toString().split(",");
                if (xy.length != 2) continue;

                sumX += Double.parseDouble(xy[0].trim());
                sumY += Double.parseDouble(xy[1].trim());
                count++;
            }

            if (count == 0) return; // no points assigned

            double cx = sumX / count;
            double cy = sumY / count;

            outValue.set(cx + "," + cy);
            context.write(key, outValue);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: hadoop jar <jar> org.example.KMeansMultiIteration <pointsIn> <centroids> <outBase> <K> <R>");
            System.exit(2);
        }

        String inPoints = args[0];
        String initCentroids = args[1];
        String outBase = args[2];
        int K = Integer.parseInt(args[3]);
        int R = Integer.parseInt(args[4]);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path currCentroids = new Path(initCentroids);

        for (int iter = 1; iter <= R; iter++) {
            Job job = Job.getInstance(conf, "kmeans-iter-" + iter);
            job.setJarByClass(KMeansMultiIteration.class);

            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            job.setNumReduceTasks(K);

            // IMPORTANT: use the symlink name you read ("centroids")
            job.addCacheFile(new URI(currCentroids.toString() + "#centroids"));

            FileInputFormat.addInputPath(job, new Path(inPoints));

            Path outIteration = new Path(outBase + "/iter" + iter);
            if (fs.exists(outIteration)) fs.delete(outIteration, true);
            FileOutputFormat.setOutputPath(job, outIteration);

            if (!job.waitForCompletion(true)) System.exit(1);

            Path merged = new Path(outBase + "/centroids_iter" + iter + ".txt");
            if (fs.exists(merged)) fs.delete(merged, true);
            mergeReducersToSingleCentroidFile(fs, outIteration, merged);

            currCentroids = merged;
        }
    }

    private static void mergeReducersToSingleCentroidFile(FileSystem fs, Path outIter, Path merged) throws IOException {
        try (FSDataOutputStream out = fs.create(merged, true)) {
            RemoteIterator<LocatedFileStatus> it = fs.listFiles(outIter, false);
            while (it.hasNext()) {
                Path p = it.next().getPath();
                String name = p.getName();
                if (!name.startsWith("part-")) continue;
                try (FSDataInputStream in = fs.open(p)) {
                    byte[] buf = new byte[64 * 1024];
                    int r;
                    while ((r = in.read(buf)) > 0) out.write(buf, 0, r);
                }
            }
        }
    }
}
