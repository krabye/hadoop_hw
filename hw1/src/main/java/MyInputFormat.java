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
        int ndocs;
        int cur_doc;
        Text cur_text;
        int max_doc_size;

        byte[] value;
        List<Integer> docs_size;
        long offset;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            docs_size = new ArrayList<>();
            ndocs = 0;
            cur_doc = 0;
            max_doc_size = 0;
            offset = 0;

            Configuration conf = context.getConfiguration();
            FileSplit fsplit = (FileSplit)split;
            Path path = fsplit.getPath();
            FileSystem fs = path.getFileSystem(conf);
            input = fs.open(path);
            offset = fsplit.getStart();
            long end = offset + fsplit.getLength();
            input.seek(offset);

            DataInputStream input_idx = new DataInputStream(fs.open(new Path(path.toString()+".idx")));
            try {
                long total_offset = 0;
                while (true) {
                    int s = 0;
                    for (int i = 0; i < 4; ++i)
                        s += input_idx.readUnsignedByte() << (i*8);

                    if(s > max_doc_size)
                        max_doc_size = s;

                    if (total_offset >= offset) {
                        docs_size.add(s);
                        ndocs++;
                    }
                    total_offset += s;

                    if (total_offset >= end)
                        break;
                }
            } catch (EOFException ignored) {
            }
            input_idx.close();

            value = new byte[max_doc_size+1];
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (cur_doc >= ndocs)
                return false;

            try {
                IOUtils.readFully(input, value, 0, docs_size.get(cur_doc));
            } catch (IOException e){
                return false;
            }

            Inflater decompresser = new Inflater();
            decompresser.setInput(value, 0, docs_size.get(cur_doc));
            byte[] result = new byte[100*max_doc_size];
            int resultLength = 0;
            try {
                resultLength = decompresser.inflate(result);
            } catch (DataFormatException e) {
                e.printStackTrace();
            }
            decompresser.end();

            // Decode the bytes into a String
            cur_text = new Text(new String(result, 0, resultLength, "UTF-16BE"));
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
        MyRecordReader reader = new MyRecordReader();
//        reader.initialize(split, context);
        return reader;
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        List<InputSplit> splits = new ArrayList<>();

        for (FileStatus status: listStatus(context)) {

            Path path = status.getPath();
            Configuration conf = context.getConfiguration();
            FileSystem fs = path.getFileSystem(conf);
            FSDataInputStream input = fs.open(new Path(path.toString()+".idx"));
            DataInputStream idx = new DataInputStream(input);

            long split_size = getNumBytesPerSplit(conf);
            long cur_split_size = 0;
            long offset = 0;
            long ndocs = 0;

            try {
                while (true) {
                    long s = 0;
                    for (int i = 0; i < 4; i++)
                        s += idx.readUnsignedByte() << (i * 8);;

                    if (cur_split_size + s <= split_size){
                        cur_split_size += s;
                        ndocs++;
                    } else {
                        splits.add(new FileSplit(path, offset, cur_split_size, null));
                        offset += cur_split_size;
                        cur_split_size = s;
                        ndocs = 0;
                    }
                }
            } catch (EOFException ignored) {
                splits.add(new FileSplit(path, offset, cur_split_size, null));
            }
            idx.close();
            input.close();
        }

        return splits;
    }

    private static final String BYTES_PER_MAP = "mapreduce.input.indexedgz.bytespermap";

    private static long getNumBytesPerSplit(Configuration conf) {
        return conf.getLong(BYTES_PER_MAP, 104857600);
    }
}
