package com.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Production-grade WordCount MapReduce Application.
 * Demonstrates the Mapper, Reducer, Combiner, and a custom Partitioner.
 */
public class WordCount extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

    /**
     * Mapper Implementation.
     * Receives byte offset (LongWritable) and text line (Text).
     * Emits word (Text) and a count of 1 (IntWritable).
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            LOG.info("Initializing Mapper task: {}", context.getTaskAttemptID());
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Clean punctuation and tokenize line
            String line = value.toString().replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase();
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken().trim();
                if (!token.isEmpty()) {
                    word.set(token);
                    context.write(word, one);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            LOG.info("Mapper task cleanup: {}", context.getTaskAttemptID());
        }
    }

    /**
     * Custom Partitioner.
     * Partitions intermediate keys to reducers based on the starting character.
     * Words starting with a-m are sent to Reducer 0, others to Reducer 1 (if 2 reducers are configured).
     */
    public static class AlphabetPartitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            if (numPartitions <= 0) {
                return 0;
            }
            String word = key.toString();
            if (word.isEmpty()) {
                return 0;
            }
            char firstChar = word.charAt(0);
            
            // If only 1 partition is available, map everything to 0
            if (numPartitions == 1) {
                return 0;
            }

            // Distribute across mappers/reducers based on character ranges
            if (Character.isLetter(firstChar)) {
                if (firstChar >= 'a' && firstChar <= 'm') {
                    return 0;
                } else {
                    return 1 % numPartitions;
                }
            }
            // Non-alphabetic tokens go to the last partition
            return (numPartitions - 1);
        }
    }

    /**
     * Reducer Implementation.
     * Receives unique word (Text) and list of counts (Iterable<IntWritable>).
     * Computes the sum and emits word (Text) and total count (IntWritable).
     */
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            LOG.info("Initializing Reducer task: {}", context.getTaskAttemptID());
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            LOG.info("Reducer task cleanup: {}", context.getTaskAttemptID());
        }
    }

    /**
     * Execution driver.
     */
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordCount <in> <out>");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration conf = getConf();
        
        // Define Job Name
        Job job = Job.getInstance(conf, "Production WordCount MapReduce Job");
        job.setJarByClass(WordCount.class);

        // Set Mapper, Combiner, Partitioner, and Reducer classes
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class); // Local aggregation optimization
        job.setPartitionerClass(AlphabetPartitioner.class);
        job.setReducerClass(IntSumReducer.class);

        // Set Key and Value Output Classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input and Output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Crucial Production Configurations: Set number of Reducers explicitly to match Partitioner
        // In this case, 2 Reducers since our custom partitioner partitions into [0] (a-m) and [1] (n-z)
        int numReducers = conf.getInt("mapreduce.job.reduces", 2);
        job.setNumReduceTasks(numReducers);
        LOG.info("Setting number of reducers to: {}", numReducers);

        // Define HDFS input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Compress Reducer Outputs for best production practice
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.GzipCodec.class);

        LOG.info("Submitting WordCount MapReduce job...");
        boolean success = job.waitForCompletion(true);
        
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(exitCode);
    }
}
