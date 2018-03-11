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
        int max_doc_size = 0;

        byte[] value;
        List<Integer> docs_size = new ArrayList<>();
        long offset = 0;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
//            System.out.println("capacity"+max_doc_size);
//            value.setCapacity(1000000);

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

                    if (s < 0) {
                        throw new IOException("Index < 0: " + s);
                    }

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
            if (max_doc_size == 0)
                throw new IOException("MAX DOC SIZE is 0");
            System.out.println("initialize, ndocs: " + ndocs);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            System.out.println("Nextkey, cur_doc: " + cur_doc + ", ndocs: " + ndocs);
            if (cur_doc >= ndocs)
                return false;

            try {
                IOUtils.readFully(input, value, 0, docs_size.get(cur_doc));
            } catch (IOException e){
                return false;
            }
//            offset += docs_size.get(cur_doc);
            Inflater decompresser = new Inflater();
            if (docs_size.get(cur_doc) > value.length || docs_size.get(cur_doc) < 0)
                throw new IOException("cur doc size greater max doc size" + docs_size.get(cur_doc) + value.length );
            decompresser.setInput(value, 0, docs_size.get(cur_doc));
            byte[] result = new byte[100*max_doc_size];
//            result.setCapacity(1000000*100);
            int resultLength = 0;
            try {
                resultLength = decompresser.inflate(result);
            } catch (DataFormatException e) {
                e.printStackTrace();
            }
            decompresser.end();

            // Decode the bytes into a String
            cur_text = new Text(new String(result, 0, resultLength));
            cur_doc++;
//            System.out.println("End nextKeyValue");
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
        reader.initialize(split, context);
        return reader;
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
            long ndocs = 0;

            try {
                while (true) {
                    long s = 0;
                    for (int i = 0; i < 4; i++) {
//                        System.out.println("tmp"+i+": "+tmp);
                        s += idx.readUnsignedByte() << (i * 8);;
                    }

                    if (s < 0) {
                        throw new IOException("Index < 0: " + s);
                    }

                    if (cur_split_size + s <= split_size){
                        cur_split_size += s;
                        ndocs++;
                    } else {
                        splits.add(new FileSplit(path, offset, cur_split_size, null));
                        offset += cur_split_size;
                        cur_split_size = s;
                        System.out.println("split ndocs: " + ndocs);
                        System.out.println("offset: "+offset+", split_size: "+cur_split_size);
                        ndocs = 0;
                    }

//                    System.out.println(s);
                }
            } catch (EOFException ignored) {
                splits.add(new FileSplit(path, offset, cur_split_size, null));
                System.out.println("split ndocs: " + ndocs);
            }
            idx.close();
            input.close();
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
