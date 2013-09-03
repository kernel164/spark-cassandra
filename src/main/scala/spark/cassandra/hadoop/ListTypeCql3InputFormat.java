package spark.cassandra.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

public class ListTypeCql3InputFormat extends InputFormat<List<?>, NullWritable> implements org.apache.hadoop.mapred.InputFormat<List<?>, NullWritable> {
    public static final String MAPRED_TASK_ID = "mapred.task.id";

    @Override
    public RecordReader<List<?>, NullWritable> getRecordReader(InputSplit split, JobConf jobConf, final Reporter reporter) throws IOException {
        TaskAttemptContext tac = new TaskAttemptContext(jobConf, TaskAttemptID.forName(jobConf.get(MAPRED_TASK_ID))) {
            @Override
            public void progress() {
                reporter.progress();
            }
        };
        ListTypeCql3RecordReader recordReader = new ListTypeCql3RecordReader();
        recordReader.initialize((org.apache.hadoop.mapreduce.InputSplit) split, tac);
        return recordReader;
    }

    @Override
    public InputSplit[] getSplits(JobConf jobConf, int arg1) throws IOException {
        TaskAttemptContext tac = new TaskAttemptContext(jobConf, new TaskAttemptID());
        List<org.apache.hadoop.mapreduce.InputSplit> newInputSplits = this.getSplits(tac);
        org.apache.hadoop.mapred.InputSplit[] oldInputSplits = new org.apache.hadoop.mapred.InputSplit[newInputSplits.size()];
        for (int i = 0; i < newInputSplits.size(); i++)
            oldInputSplits[i] = (Cql3RecordSplit) newInputSplits.get(i);
        return oldInputSplits;
    }

    @Override
    public org.apache.hadoop.mapreduce.RecordReader<List<?>, NullWritable> createRecordReader(org.apache.hadoop.mapreduce.InputSplit inputSplit,
            TaskAttemptContext context) throws IOException, InterruptedException {
        return new ListTypeCql3RecordReader();
    }

    @Override
    public List<org.apache.hadoop.mapreduce.InputSplit> getSplits(JobContext context) throws IOException {
        String query = context.getConfiguration().get("cql3.key.query");
        return null;
    }

    public class Cql3RecordSplit extends org.apache.hadoop.mapreduce.InputSplit implements Writable, org.apache.hadoop.mapred.InputSplit {
        public Cql3RecordSplit() {
        }

        @Override
        public void readFields(DataInput arg0) throws IOException {
        }

        @Override
        public void write(DataOutput arg0) throws IOException {
        }

        @Override
        public long getLength() throws IOException {
            return 0;
        }

        @Override
        public String[] getLocations() throws IOException {
            return null;
        }
    }
}
