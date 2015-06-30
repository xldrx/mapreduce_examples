import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TopWords {
    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }

    public static class WordCountMap extends Mapper<Object, Text, Text, IntWritable> {
        List<String> commonWords = Arrays.asList("the", "a", "an", "and", "of", "to", "in", "am", "is", "are", "at", "not");
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, " \t,;.?!-:@[](){}_*/");
            while (tokenizer.hasMoreTokens()) {
                String nextToken = tokenizer.nextToken();
                if (!commonWords.contains(nextToken.trim().toLowerCase())) {
                    context.write(new Text(nextToken), new IntWritable(1));
                }
            }
        }
    }

    public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class TopWordsMap extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
        private TreeMap<Integer, Text> countToWordMap = new TreeMap<Integer, Text>();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Integer count = Integer.parseInt(value.toString());
            countToWordMap.put(count, new Text(key));

            if (countToWordMap.size() > 10) {
                countToWordMap.remove(countToWordMap.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Integer key : countToWordMap.keySet()) {
                String[] strings = {countToWordMap.get(key).toString(), Integer.toString(key)};
                TextArrayWritable val = new TextArrayWritable(strings);
                context.write(NullWritable.get(), val);
            }
        }
    }

    public static class TopWordsReduce extends Reducer<NullWritable, TextArrayWritable, Text, IntWritable> {
        private TreeMap<Integer, Text> countToWordMap = new TreeMap<Integer, Text>();

        @Override
        public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
            for (TextArrayWritable val: values) {
                Text[] pair= (Text[]) val.toArray();
                Text word = pair[0];
                Integer count = Integer.parseInt(pair[1].toString());

                countToWordMap.put(count, word);

                if (countToWordMap.size() > 10) {
                    countToWordMap.remove(countToWordMap.firstKey());
                }
            }

            for (Integer count : countToWordMap.keySet()) {
                Text word = countToWordMap.get(count);
                IntWritable value = new IntWritable(count);
                context.write(word, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/tmp");
        fs.delete(tmpPath, true);


        Job jobA = Job.getInstance(conf, "wordcount");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(WordCountMap.class);
        jobA.setReducerClass(WordCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(TopWords.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Words");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(TopWordsMap.class);
        jobB.setReducerClass(TopWordsReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopWords.class);
        System.exit(jobB.waitForCompletion(true) ? 0 : 1);
    }
}