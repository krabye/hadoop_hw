import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
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

public class MyInputFormat extends FileInputFormat<NullWritable, BytesWritable>{
    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
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
//            BytesWritable value = new BytesWritable();
//            input.readFully(0, value.getBytes());

            DataInputStream idx = new DataInputStream(input);

            try {
                while (true) {
                    int s = 0;
                    for (int i = 0; i < 4; ++i)
                        s += idx.readByte() << (i*8);

                    System.out.println(s);
                }
            } catch (EOFException ignored) {
                System.out.println("[EOF]");
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
}
