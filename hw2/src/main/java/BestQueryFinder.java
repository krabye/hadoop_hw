import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


// Обрабатываем статистику GSOD используя сортировку средствами mapreduce
// Задача является демонстрационной и не нацелена быть оптимальной
public class BestQueryFinder extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new BestQueryFinder(), args);
        System.exit(rc);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(getConf(), args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static class HostQueryPartitioner extends Partitioner<TextPair, IntWritable> {
        @Override
        public int getPartition(TextPair key, IntWritable val, int numPartitions) {
            // Host -> 1 reducer
            return Math.abs(key.getHost().hashCode()) % numPartitions;
        }
    }

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(TextPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((TextPair)a).compareTo((TextPair)b);
        }
    }

    public static class HostQueryGrouper extends WritableComparator {
        protected HostQueryGrouper() {
            super(TextPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((TextPair) a).getHost().compareTo(((TextPair) b).getHost());
        }
    }

    public static class HQMapper extends Mapper<LongWritable, Text, TextPair, IntWritable>
    {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String query = line.split("\t")[0];
            String host = line.split("\t")[1];

            Pattern pattern = Pattern.compile("^(([^:/?#]+):)?(/?/?([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");
            Matcher matcher = pattern.matcher(host);

            if (matcher.find()) {
                host = matcher.group(4);
            } else {
                return;
            }

            context.write(new TextPair(host, query), new IntWritable(1));
        }
    }


    public static class HQReducer extends Reducer<TextPair, IntWritable, Text, Text> {
        @Override
        protected void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int maxCount = 0;
            int curCount = 0;
            String maxQuery = "";
            String curQuery = "";
            String host = key.getHost().toString();

            for (IntWritable v : values) {
                if (!curQuery.equals(key.getQuery().toString())) {
                    if (curCount > maxCount){
                        maxCount = curCount;
                        maxQuery = curQuery;
                    }
                    curQuery = key.getQuery().toString();
                    curCount = 0;
                } else {
                    curCount += v.get();
                }
            }

            if (maxCount >= getMinClicks(context.getConfiguration()))
                context.write(new Text(host+"\t"+maxQuery), new Text(String.valueOf(maxCount)));
        }
    }

    Job GetJobConf(Configuration conf, String input, String out_dir) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(BestQueryFinder.class);
        job.setJobName(BestQueryFinder.class.getCanonicalName());

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(out_dir));

        job.setMapperClass(HQMapper.class);
        job.setReducerClass(HQReducer.class);

        job.setPartitionerClass(HostQueryPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(HostQueryGrouper.class);

        // выход mapper-а != вывод reducer-а, поэтому ставим отдельно
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    private static final String SEO_MINCLICKS = "seo.minclicks";

    private static long getMinClicks(Configuration conf) {
        return conf.getLong(SEO_MINCLICKS, 1);
    }
}