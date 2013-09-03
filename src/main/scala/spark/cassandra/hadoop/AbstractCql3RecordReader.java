package spark.cassandra.hadoop;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class AbstractCql3RecordReader<K, V> extends RecordReader<K, V> implements org.apache.hadoop.mapred.RecordReader<K, V> {

    @Override
    public K createKey() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public V createValue() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long getPos() throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean next(K arg0, V arg1) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public K getCurrentKey() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public V getCurrentValue() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public float getProgress() throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean nextKeyValue() throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

}
