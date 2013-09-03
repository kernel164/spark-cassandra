package spark.cassandra.hadoop;

import java.io.IOException;
import java.util.Map;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.Progressable;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;

public class MapTypeCql3OutputFormat extends OutputFormat<Map<String, Object>, Map<String, Object>> implements
        org.apache.hadoop.mapred.OutputFormat<Map<String, Object>, Map<String, Object>> {

    @Override
    public void checkOutputSpecs(JobContext context) {
        checkOutputSpecs(context.getConfiguration());
    }

    @Override
    @Deprecated
    public void checkOutputSpecs(FileSystem filesystem, JobConf job) throws IOException {
        checkOutputSpecs(job);
    }

    protected void checkOutputSpecs(Configuration conf) {
        if (CqlConfigHelper.getOutputCql(conf) == null)
            throw new UnsupportedOperationException("You must set the output cql");
        if (ConfigHelper.getOutputInitialAddress(conf) == null)
            throw new UnsupportedOperationException("You must set the initial output address to a Cassandra node");
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new NullOutputCommitter();
    }

    /** Fills the deprecated OutputFormat interface for streaming. */
    @Deprecated
    public Cql3RecordWriter getRecordWriter(FileSystem filesystem, JobConf job, String name, org.apache.hadoop.util.Progressable progress) throws IOException {
        return new Cql3RecordWriter(job, new Progressable(progress));
    }

    /**
     * Get the {@link RecordWriter} for the given task.
     * 
     * @param context
     *            the information about the current task.
     * @return a {@link RecordWriter} to write the output for the job.
     * @throws IOException
     */
    public Cql3RecordWriter getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
        return new Cql3RecordWriter(context);
    }

    static class Cql3RecordWriter extends AbstractCql3RecordWriter<Map<String, Object>, Map<String, Object>> {

        Cql3RecordWriter(TaskAttemptContext context) throws IOException {
            this(context.getConfiguration());
            this.progressable = new Progressable(context);
        }

        Cql3RecordWriter(Configuration conf, Progressable progressable) throws IOException {
            this(conf);
            this.progressable = progressable;
        }

        Cql3RecordWriter(Configuration conf) {
            super(conf);
        }

        @Override
        public void write(Map<String, Object> keys, Map<String, Object> values) {
            ColumnDefinitions variables = ps.getVariables();
            Object[] bvals = new Object[variables.size()];
            for (int i = 0; i < bvals.length; i++) {
                String name = variables.getName(i);
                Object obj = keys.get(name);
                if (obj == null && values != null)
                    obj = values.get(name);
                if (obj != null)
                    bvals[i] = obj;
            }
            BoundStatement bstmt = ps.bind(bvals);
            session.execute(bstmt);
            progressable.progress();
        }
    }
}
