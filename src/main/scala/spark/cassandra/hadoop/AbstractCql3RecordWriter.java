package spark.cassandra.hadoop;

import java.io.IOException;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.Progressable;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public abstract class AbstractCql3RecordWriter<K, Y> extends RecordWriter<K, Y> implements org.apache.hadoop.mapred.RecordWriter<K, Y> {
    protected final Configuration conf;
    protected final ConsistencyLevel consistencyLevel;
    protected Progressable progressable;
    protected final Cluster cluster;
    protected final Session session;
    protected PreparedStatement ps;

    protected AbstractCql3RecordWriter(Configuration conf) {
        this.conf = conf;
        this.consistencyLevel = ConsistencyLevel.valueOf(ConfigHelper.getWriteConsistencyLevel(conf));

        // build cluster using the available list of cassandra hosts.
        cluster = new Cluster.Builder().addContactPoints(ConfigHelper.getOutputInitialAddress(conf).split(",")).build();

        // connect to cluster.
        session = cluster.connect();
        String cqlQuery = CqlConfigHelper.getOutputCql(conf).trim();
        ps = session.prepare(cqlQuery);
        ps.setConsistencyLevel(consistencyLevel);
    }

    /**
     * Close this <code>RecordWriter</code> to future operations, but not before flushing out the batched mutations.
     * 
     * @param context
     *            the context of the task
     * @throws IOException
     */
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        close();
        closeInternal();
    }

    /** Fills the deprecated RecordWriter interface for streaming. */
    @Deprecated
    public void close(Reporter reporter) throws IOException {
        close();
        closeInternal();
    }

    private void closeInternal() {
        cluster.shutdown();
    }

    protected void close() throws IOException {
    }
}