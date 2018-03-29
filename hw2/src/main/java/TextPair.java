import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class TextPair implements WritableComparable<TextPair> {
    private Text host;
    private Text query;

    public TextPair() {
        set(new Text(), new Text());
    }

    public TextPair(String host, String query) {
        set(new Text(host), new Text(query));
    }

    public TextPair(Text host, Text query) {
        set(host, query);
    }

    private void set(Text a, Text b) {
        host = a;
        query = b;
    }

    public Text getHost() {
        return host;
    }

    public Text getQuery() {
        return query;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        host.write(out);
        query.write(out);
    }

    @Override
    public int compareTo(@Nonnull TextPair o) {
        int cmp = host.compareTo(o.host);
        return (cmp == 0) ? query.compareTo(o.query) : cmp;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        host.readFields(dataInput);
        query.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        return host.hashCode() * 163 + query.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TextPair) {
            TextPair tp = (TextPair) obj;
            return host.equals(tp.host) && query.equals(tp.query);
        }
        return false;
    }

    @Override
    public String toString() {
        return host + "\t" + query;
    }

}