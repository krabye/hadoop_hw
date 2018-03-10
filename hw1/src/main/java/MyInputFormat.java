import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class MyInputFormat extends FileInputFormat<LongWritable, Text>{

    public class MyRecordReader extends RecordReader<LongWritable, Text> {
        FSDataInputStream input;
        int ndocs = 0;
        int cur_doc = 0;
        Text cur_text;

        BytesWritable value = new BytesWritable();
        List<Integer> docs_size = new ArrayList<>();
        long offset = 0;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            FileSplit fsplit = (FileSplit)split;
            Path path = fsplit.getPath();
            FileSystem fs = path.getFileSystem(conf);
            input = fs.open(path);
            offset = fsplit.getStart();
            input.seek(offset);

            DataInputStream input_idx = new DataInputStream(fs.open(new Path(path.toString()+".idx")));
            try {
                long total_offset = 0;
                while (true) {
                    int s = 0;
                    for (int i = 0; i < 4; ++i)
                        s += input_idx.readByte() << (i*8);

                    if (total_offset >= offset)
                        docs_size.add(s);
                    total_offset += s;
                    ndocs++;
                }
            } catch (EOFException ignored) {
            }
            input_idx.close();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (cur_doc >= ndocs)
                return false;

            IOUtils.readFully(input, value.getBytes(), 0, docs_size.get(cur_doc));
            Inflater decompresser = new Inflater();
            decompresser.setInput(value.getBytes(), (int)offset, docs_size.get(cur_doc));
            BytesWritable result = new BytesWritable();
            int resultLength = 0;
            try {
                resultLength = decompresser.inflate(result.getBytes());
            } catch (DataFormatException e) {
                e.printStackTrace();
            }
            decompresser.end();

            // Decode the bytes into a String
            cur_text = new Text(result.getBytes());
            cur_doc++;
            return true;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return new LongWritable(cur_text.hashCode());
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return cur_text;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (float)cur_doc / ndocs;
        }

        @Override
        public void close() throws IOException {
            IOUtils.closeStream(input);
        }
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return null;
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        List<InputSplit> splits = new ArrayList<>();

        for (FileStatus status: listStatus(context)) {
            System.out.println("Path " + status.getPath());

            Path path = status.getPath();
            Configuration conf = context.getConfiguration();
            FileSystem fs = path.getFileSystem(conf);
            FSDataInputStream input = fs.open(new Path(path.toString()+".idx"));
            DataInputStream idx = new DataInputStream(input);

            long split_size = getNumBytesPerSplit(conf);
            long cur_split_size = 0;
            long offset = 0;

            try {
                while (true) {
                    long s = 0;
                    for (int i = 0; i < 4; ++i)
                        s += idx.readByte() << (i*8);

                    if (cur_split_size + s <= split_size){
                        cur_split_size += s;
                    } else {
                        splits.add(new FileSplit(path, offset, cur_split_size, null));
                        offset += cur_split_size;
                        cur_split_size = 0;
                    }

//                    System.out.println(s);
                }
            } catch (EOFException ignored) {
                splits.add(new FileSplit(path, offset, cur_split_size, null));
            }
            idx.close();
//            long split_size = getNumBytesPerSplit(context.getConfiguration());
//            long flen = status.getLen();
//            Path path = status.getPath();
//            System.out.println("flen: " + flen + ", split_size: " + split_size + ", n_splits:" + (flen/split_size));
//
//            int n_splits = (int)(flen/split_size);
//            long offset = 0;
//
//            for (int i=0; i < n_splits; ++i){
//                splits.add(new FileSplit(path, offset, split_size, null));
//                offset += split_size;
//            }

            /*
             * WRITE YOUR CODE HERE:
             * you have to create splits using
             * splits.add(new FileSplit(path, offset, size, null));
             */
        }

        return splits;
    }

    public static final String BYTES_PER_MAP = "mapreduce.input.indexedgz.bytespermap";

    public static long getNumBytesPerSplit(Configuration conf) {
        return conf.getLong(BYTES_PER_MAP, 104857600);
    }
}
