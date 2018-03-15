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
//        Pattern pattern = Pattern.compile("[\\p{L}" + String.valueOf(((char) 775)) + "]+");
        Pattern pattern = Pattern.compile("\\p{L}");
        static final LongWritable one = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Matcher matcher = pattern.matcher(line);
            HashSet<Text> hs = new HashSet<>();
            while (matcher.find()) {
                hs.add(new Text(matcher.group().toLowerCase()));
            }
            for (Text t: hs){
                context.write(t, one);
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text word, Iterable<LongWritable> nums, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable i: nums)
                count += i.get();

            context.write(word, new LongWritable(count));
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(WordCountJob.class);
        job.setJobName(WordCountJob.class.getCanonicalName());

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
