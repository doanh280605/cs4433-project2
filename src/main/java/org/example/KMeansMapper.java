package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.net.URI;

import java.io.*;
import java.util.*;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
    private final Map<Integer, double[]> centroids = new HashMap<>();
    private final IntWritable outKey = new IntWritable();
    private final Text outValue = new Text();

    @Override
    protected void setup(Context context) throws IOException {
        URI[] files = context.getCacheFiles();
        if (files == null || files.length == 0) {
            throw new IOException("Centroid file not found");
        }

        // read centroid file that was added
        File centroidFile = new File(files[0].getPath());
        try (BufferedReader br = new BufferedReader(new FileReader("centroids"))) {
            String line;
            while((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                // expected clusterID
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
            throw new IOException("No centroid file found");
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // expected point line: x, y
        String s = value.toString().trim();
        if (s.isEmpty()) return;

        String[] xy = s.split(",");
        if (xy.length != 2) return; // skip malformed lines

        double x = Double.parseDouble(xy[0].trim());
        double y = Double.parseDouble(xy[1].trim());

        int bestCid = -1;
        double bestDistance = Double.MAX_VALUE;

        for (Map.Entry<Integer, double[]> e: centroids.entrySet()) {
            int cid = e.getKey();
            double[] c = e.getValue();
            double dx = x - c[0];
            double dy = y - c[1];
            double distance = dx * dx + dy * dy;

            if (distance < bestDistance) {
                bestDistance = distance;
                bestCid = cid;
            }
        }

        outKey.set(bestCid);
        outValue.set(x + "," + y);
        context.write(outKey, outValue);
    }
}
