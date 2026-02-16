package org.example;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

public class DataGen {
    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("Usage: java org.example.DataGen <numPoints> <K> <outDir> [seed]");
            System.exit(1);
        }

        int numPoints = Integer.parseInt(args[0]);
        if (numPoints < 3000) {
            throw new IllegalArgumentException("numPoints must be >= 3000");
        }

        int K = Integer.parseInt(args[1]);
        if (K <= 0) {
            throw new IllegalArgumentException("K must be > 0");
        }

        Path outDir = Paths.get(args[2]); // Java 8
        long seed = (args.length >= 4) ? Long.parseLong(args[3]) : System.nanoTime();
        Random rand = new Random(seed);

        Files.createDirectories(outDir);

        Path pointsPath = outDir.resolve("points.csv");
        Path seedsPath = outDir.resolve("seeds.txt");

        writePoints(pointsPath, numPoints, rand);
        writeSeeds(seedsPath, K, rand);

        System.out.println("Done.");
        System.out.println("Seed used: " + seed);
        System.out.println("Wrote: " + pointsPath.toAbsolutePath());
        System.out.println("Wrote: " + seedsPath.toAbsolutePath());
    }

    private static void writePoints(Path path, int n, Random rand) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(path)) {
            for (int i = 0; i < n; i++) {
                int x = rand.nextInt(5001); // 0..5000
                int y = rand.nextInt(5001);
                w.write(x + "," + y);
                w.newLine();
            }
        }
    }

    private static void writeSeeds(Path path, int k, Random rand) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(path)) {
            for (int cid = 0; cid < k; cid++) {
                int x = rand.nextInt(5001); // 0..5000
                int y = rand.nextInt(5001);
                w.write(cid + "\t" + x + "," + y);
                w.newLine();
            }
        }
    }
}
