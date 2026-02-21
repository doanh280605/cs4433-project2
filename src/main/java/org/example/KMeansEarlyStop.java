package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.*;

/**
 * This implementation performs an advanced multi-iteration KMeans clustering algorithm
 * using multiple MapReduce jobs with an early-stopping convergence criterion.
 *
 * Input:
 * - Fixed dataset of 2D points stored in HDFS
 * - Initial centroid file with K cluster centers stored in HDFS
 * - Defined maximum number of iterations R
 * - Convergence threshold ε (epsilon) specifying the minimum centroid movement
 *
 * Process:
 * - The main program controls the iteration loop
 * - Each iteration launches one MapReduce job
 * - The output centroids from iteration i are merged and used as input centroids
 *   for iteration i+1
 * - After each iteration, the algorithm computes the maximum movement between
 *   old and new centroids
 * - If the maximum centroid movement is less than or equal to ε, the algorithm
 *   terminates early
 * - If convergence is not reached, the algorithm stops after R iterations
 *
 * Mapper:
 * - Loads the current iteration’s centroids using Hadoop’s Distributed Cache
 * - For each input point, computes the Euclidean distance to all centroids
 * - Assigns the point to the nearest centroid
 * - Emits (clusterId, point) pairs
 *
 * Reducer:
 * - Receives all points assigned to the same clusterId
 * - Computes the new centroid by averaging the x and y coordinates
 * - Outputs (clusterId, new centroid coordinates)
 *
 * Output:
 * - For each completed iteration, a new centroid file in HDFS
 * - A convergence status file indicating whether early stopping occurred
 * - The final centroid file represents the result of the algorithm
 *
 * This implementation extends the fixed-iteration KMeans by incorporating
 * a convergence-based stopping condition, reducing unnecessary iterations
 * when cluster centroids stabilize.
 */

public class KMeansEarlyStop {

    public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final Map<Integer, double[]> centroids = new HashMap<>();
        private final IntWritable outKey = new IntWritable();
        private final Text outVal = new Text();

        @Override
        protected void setup(Context context) throws IOException {
            // Use Distributed Cache symlink named "centroids"
            try (BufferedReader br = new BufferedReader(new FileReader("centroids"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;
                    String[] parts = line.split("\\t");
                    int cid = Integer.parseInt(parts[0].trim());
                    String[] xy = parts[1].trim().split(",");
                    centroids.put(cid, new double[]{Double.parseDouble(xy[0]), Double.parseDouble(xy[1])});
                }
            }
            if (centroids.isEmpty()) throw new IOException("No centroids loaded");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString().trim();
            if (s.isEmpty()) return;

            String[] xy = s.split(",");
            if (xy.length != 2) return;
            double x = Double.parseDouble(xy[0].trim());
            double y = Double.parseDouble(xy[1].trim());

            int bestCid = -1;
            double best = Double.MAX_VALUE;
            for (Map.Entry<Integer, double[]> e : centroids.entrySet()) {
                double[] c = e.getValue();
                double dx = x - c[0], dy = y - c[1];
                double d = dx * dx + dy * dy;
                int cid = e.getKey();
                if (d < best || (d == best && cid < bestCid)) {
                    best = d;
                    bestCid = cid;
                }
            }

            outKey.set(bestCid);
            outVal.set(x + "," + y);
            context.write(outKey, outVal);
        }
    }

    public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private final Text outVal = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double sumX = 0.0, sumY = 0.0;
            long n = 0;
            for (Text v : values) {
                String[] xy = v.toString().split(",");
                if (xy.length != 2) continue;
                sumX += Double.parseDouble(xy[0].trim());
                sumY += Double.parseDouble(xy[1].trim());
                n++;
            }
            if (n == 0) return;
            outVal.set((sumX / n) + "," + (sumY / n));
            context.write(key, outVal);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            System.err.println("Usage: hadoop jar <jar> org.example.KMeansEarlyStop <pointsIn> <centroids0> <outBase> <K> <R> <epsilon>");
            System.exit(2);
        }

        String pointsIn = args[0];
        String centroids0 = args[1];
        String outBase = args[2];
        int K = Integer.parseInt(args[3]);
        int R = Integer.parseInt(args[4]);
        double epsilon = Double.parseDouble(args[5]); // early-stop threshold

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path curCentroids = new Path(centroids0);

        boolean converged = false;

        for (int iter = 1; iter <= R; iter++) {
            Job job = Job.getInstance(conf, "kmeans-earlystop-iter-" + iter);
            job.setJarByClass(KMeansEarlyStop.class);

            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            job.setNumReduceTasks(K);

            // Put centroids into Distributed Cache as local file name "centroids"
            job.addCacheFile(new URI(curCentroids.toString() + "#centroids"));

            FileInputFormat.addInputPath(job, new Path(pointsIn));

            Path outIter = new Path(outBase + "/iter" + iter);
            if (fs.exists(outIter)) fs.delete(outIter, true);
            FileOutputFormat.setOutputPath(job, outIter);

            if (!job.waitForCompletion(true)) System.exit(1);

            // Merge reducer outputs into one centroid file for the next iteration
            Path merged = new Path(outBase + "/centroids_iter" + iter + ".txt");
            if (fs.exists(merged)) fs.delete(merged, true);
            mergeReducersToSingleCentroidFile(fs, outIter, merged);

            // Early stop check: compare old vs new centroids
            double maxShift = maxCentroidShift(fs, curCentroids, merged);
            if (maxShift <= epsilon) {
                converged = true;
                curCentroids = merged;
                break;
            }

            curCentroids = merged;
        }

        // Optional: write a tiny status file for reporting
        Path status = new Path(outBase + "/convergence_status.txt");
        try (FSDataOutputStream out = fs.create(status, true)) {
            out.writeBytes("CONVERGED\t" + (converged ? "YES" : "NO") + "\n");
            out.writeBytes("EPSILON\t" + epsilon + "\n");
            out.writeBytes("FINAL_CENTROIDS\t" + curCentroids + "\n");
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

    private static Map<Integer, double[]> loadCentroids(FileSystem fs, Path p) throws IOException {
        Map<Integer, double[]> m = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] parts = line.split("\\t");
                int cid = Integer.parseInt(parts[0].trim());
                String[] xy = parts[1].trim().split(",");
                m.put(cid, new double[]{Double.parseDouble(xy[0]), Double.parseDouble(xy[1])});
            }
        }
        return m;
    }

    private static double maxCentroidShift(FileSystem fs, Path oldC, Path newC) throws IOException {
        Map<Integer, double[]> a = loadCentroids(fs, oldC);
        Map<Integer, double[]> b = loadCentroids(fs, newC);

        double max = 0.0;
        for (Map.Entry<Integer, double[]> e : b.entrySet()) {
            int cid = e.getKey();
            double[] nb = e.getValue();
            double[] oa = a.get(cid);
            if (oa == null) continue; // if a cluster vanished, ignore here (or treat as large shift)
            double dx = nb[0] - oa[0];
            double dy = nb[1] - oa[1];
            double shift = Math.sqrt(dx * dx + dy * dy);
            if (shift > max) max = shift;
        }
        return max;
    }
}