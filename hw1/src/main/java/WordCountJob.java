import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCountJob extends Configured implements Tool {
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        Pattern pattern = Pattern.compile("\\p{L}+");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Matcher matcher = pattern.matcher(line);
            // split by space symbols (space, tab, ...)
            while (matcher.find()) {
                context.write(new Text(matcher.group().toLowerCase()), key);
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text word, Iterable<LongWritable> nums, Context context) throws IOException, InterruptedException {
            int sum = 0;
            HashSet<LongWritable> hs = new HashSet<>();
            for (LongWritable i: nums)
                hs.add(i);

            // produce pairs of "word" <-> amount
            context.write(word, new LongWritable(hs.size()));
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(WordCountJob.class);
        job.setJobName(WordCountJob.class.getCanonicalName());

        // will use traditional TextInputFormat to split line-by-line
        job.setInputFormatClass(MyInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new WordCountJob(), args);
        System.exit(ret);
    }
}
