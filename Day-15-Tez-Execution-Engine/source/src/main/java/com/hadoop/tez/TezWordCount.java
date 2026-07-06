package com.hadoop.tez;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSink;
import org.apache.tez.dag.api.DataSource;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.processor.SimpleProcessor;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Enterprise implementation of an Apache Tez DAG application.
 * Programmatically constructs a Directed Acyclic Graph (DAG) for Word Count
 * with explicit Tokenizer and Summation Vertices connected via a Scatter-Gather edge.
 */
public class TezWordCount extends Configured implements Tool {

    // 1. Tokenizer Processor (Vertex 1)
    public static class TokenProcessor extends SimpleProcessor {
        public TokenProcessor(ProcessorContext context) {
            super(context);
        }

        @Override
        public void run() throws Exception {
            // Read from HDFS input data source (mapped from MRInput)
            MRInput input = (MRInput) getInputs().values().iterator().next();
            KeyValueReader kvReader = input.getReader();

            // Write intermediate output keys to Scatter-Gather edge
            KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().values().iterator().next();

            Text word = new Text();
            IntWritable one = new IntWritable(1);

            while (kvReader.next()) {
                Text line = (Text) kvReader.getCurrentValue();
                StringTokenizer itr = new StringTokenizer(line.toString());
                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase());
                    if (word.getLength() > 0) {
                        kvWriter.write(word, one);
                    }
                }
            }
        }
    }

    // 2. Summation Processor (Vertex 2)
    public static class SumProcessor extends SimpleProcessor {
        public SumProcessor(ProcessorContext context) {
            super(context);
        }

        @Override
        public void run() throws Exception {
            // Read partitioned and sorted key-values from the input Edge
            KeyValuesReader kvReader = (KeyValuesReader) getInputs().values().iterator().next();

            // Write final output aggregates to HDFS (mapped to MROutput)
            MROutput output = (MROutput) getOutputs().values().iterator().next();
            KeyValueWriter kvWriter = output.getWriter();

            while (kvReader.next()) {
                Text word = (Text) kvReader.getCurrentKey();
                int sum = 0;
                for (Object value : kvReader.getCurrentValues()) {
                    sum += ((IntWritable) value).get();
                }
                kvWriter.write(word, new IntWritable(sum));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TezWordCount <input_path> <output_path>");
            return -1;
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Configuration conf = getConf();
        TezConfiguration tezConf = new TezConfiguration(conf);

        // Define Input/Output configuration templates using MR compatibility helpers
        DataSource dataSource = MRInput.createConfigBuilder(tezConf, TextInputFormat.class, inputPath.toString()).build();
        DataSink dataSink = MROutput.createConfigBuilder(tezConf, TextOutputFormat.class, outputPath.toString()).build();

        // Configure the DAG transition Edge (Scatter-Gather partition and sort)
        OrderedPartitionedKVEdgeConfig edgeConf = OrderedPartitionedKVEdgeConfig
                .newBuilder(Text.class.getName(), IntWritable.class.getName(), HashPartitioner.class.getName())
                .build();

        // 1. Create Tokenizer Vertex
        Vertex tokenizerVertex = Vertex.create("Tokenizer",
                ProcessorDescriptor.create(TokenProcessor.class.getName()))
                .addDataSource("hdfs_input", dataSource);

        // 2. Create Summation Vertex
        Vertex summationVertex = Vertex.create("Summation",
                ProcessorDescriptor.create(SumProcessor.class.getName()))
                .addDataSink("hdfs_output", dataSink);

        // 3. Define the Directed Acyclic Graph (DAG) and link vertices with an edge
        DAG dag = DAG.create("TezWordCount")
                .addVertex(tokenizerVertex)
                .addVertex(summationVertex)
                .addEdge(Edge.create(tokenizerVertex, summationVertex, edgeConf.createDefaultEdgeProperty()));

        // 4. Initialize TezClient and submit the DAG for execution on YARN
        System.out.println("Submitting Tez WordCount DAG to YARN Resource Manager...");
        TezClient tezClient = TezClient.create("TezWordCountClient", tezConf);
        tezClient.start();

        try {
            tezClient.waitTillReady();
            org.apache.tez.client.DAGClient dagClient = tezClient.submitDAG(dag);
            org.apache.tez.dag.api.client.DAGStatus status = dagClient.waitForCompletionWithStatusUpdates(null);

            if (status.getState() == org.apache.tez.dag.api.client.DAGStatus.State.SUCCEEDED) {
                System.out.println("Tez WordCount DAG Execution Succeeded!");
                return 0;
            } else {
                System.err.println("Tez WordCount DAG failed with state: " + status.getState() + ". Diagnostics: " + status.getDiagnostics());
                return 1;
            }
        } finally {
            tezClient.stop();
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TezWordCount(), args);
        System.exit(res);
    }
}
