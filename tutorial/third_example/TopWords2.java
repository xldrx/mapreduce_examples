import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TopWords2 {
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
        @Override
        public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
            for (TextArrayWritable val: values) {
                Text[] pair= (Text[]) val.toArray();
                Text word = pair[0];
                IntWritable value = new IntWritable(Integer.parseInt(pair[1].toString()));
                context.write(word, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration());
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(TextArrayWritable.class);

        job.setMapperClass(TopWordsMap.class);
        job.setReducerClass(TopWordsReduce.class);
        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setJarByClass(TopWords2.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}