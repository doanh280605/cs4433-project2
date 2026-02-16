package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

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

        if (count == 0) return; // no points assigned

        double cx = sumX / count;
        double cy = sumY / count;

        outVal.set(cx + "," + cy);
        ctx.write(key, outVal);
    }
}
