package com.hadoop.spark;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Production-grade Spark Core Demo Application.
 * Programmatically demonstrates:
 * 1. Lazy Evaluation
 * 2. Narrow vs Wide Transformations
 * 3. Custom Partitions and HashPartitioner
 * 4. Printing RDD execution lineage (toDebugString)
 * 5. Job-Stage-Task hierarchy in actions.
 */
public class SparkWordCount {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: SparkWordCount <input-path> <output-path>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        System.out.println("=========================================================");
        System.out.println("🚀 INITIALIZING SPARK CORE DEMO APPLICATION");
        System.out.println("=========================================================");

        // Configure Spark Context
        SparkConf conf = new SparkConf()
                .setAppName("SparkCoreArchitectureDemo")
                // Kryo serialization is recommended for production-grade Spark applications
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        // Initialize Java Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);

        try {
            System.out.println("\n--- [STAGE 1] Loading Text File (Lazy Operation) ---");
            // Load text file into an RDD. 
            // Note: This operation is LAZY. No data is read yet; only the RDD metadata and dependency are built.
            JavaRDD<String> lines = sc.textFile(inputPath, 2); // Requested 2 default partitions
            System.out.println("RDD created. Class: " + lines.getClass().getName());
            System.out.println("Partitions size: " + lines.getNumPartitions());

            System.out.println("\n--- [STAGE 2] Applying Transformations (Lazy) ---");
            
            // Transformation 1: FlatMap (Narrow: 1-to-1 partition dependency, no shuffle)
            JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.toLowerCase().split("\\s+")).iterator());
            System.out.println("FlatMap RDD created (Narrow Transformation).");

            // Transformation 2: MapToPair (Narrow: 1-to-1 partition dependency, no shuffle)
            JavaPairRDD<String, Integer> wordPairs = words.mapToPair(word -> new Tuple2<>(word.replaceAll("[^a-zA-Z]", ""), 1));
            System.out.println("MapToPair RDD created (Narrow Transformation).");

            // Transformation 3: Filter (Narrow: Filter out empty words, no shuffle)
            JavaPairRDD<String, Integer> filteredPairs = wordPairs.filter(pair -> !pair._1().isEmpty());
            System.out.println("Filter RDD created (Narrow Transformation).");

            // Transformation 4: ReduceByKey with Custom Partitioner (Wide: triggers Shuffle Stage Boundary)
            // We use HashPartitioner with 3 partitions. This forces a shuffle step to distribute matching keys.
            JavaPairRDD<String, Integer> wordCounts = filteredPairs.reduceByKey(new HashPartitioner(3), (c1, c2) -> c1 + c2);
            System.out.println("ReduceByKey with HashPartitioner created (Wide Transformation - SHUFFLE boundary).");

            System.out.println("\n--- [STAGE 3] Inspecting RDD Lineage Graph (DAG) ---");
            // Printing the physical execution plan (Lineage DAG) before execution
            // We will see the stages divided by the ShuffleRDD boundary.
            System.out.println(wordCounts.toDebugString());

            System.out.println("\n--- [STAGE 4] Executing Action (Triggers Spark Job) ---");
            // Saving data triggers the Spark DAG Scheduler to create a job, split it into stages, and schedule tasks.
            System.out.println("Starting saving output to: " + outputPath);
            
            // Clean output directory if it exists (Hadoop API call handled by spark context internally or handled via shell)
            wordCounts.saveAsTextFile(outputPath);

            System.out.println("Job execution completed successfully.");
            System.out.println("=========================================================");
            
        } catch (Exception e) {
            System.err.println("Fatal error in Spark execution: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Close Spark Context to release resources
            sc.close();
        }
    }
}
