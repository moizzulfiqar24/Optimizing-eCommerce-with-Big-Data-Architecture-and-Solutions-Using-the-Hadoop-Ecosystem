import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProductHFileDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: ProductHFileDriver <input path> <output path> <table name>");
            System.exit(-1);
        }

        // Initialize HBase configuration
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "zookeeperbda");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("mapreduce.job.hbase.master", "hbasebda:16010");
        conf.set(TableOutputFormat.OUTPUT_TABLE, args[2]);

        // Set memory configurations for the MapReduce tasks
        conf.set("mapreduce.map.memory.mb", "2048");
        conf.set("mapreduce.reduce.memory.mb", "2048");
        conf.set("mapreduce.map.java.opts", "-Xmx1536m");
        conf.set("mapreduce.reduce.java.opts", "-Xmx1536m");

        // Create the MapReduce job
        Job job = Job.getInstance(conf, "Product HFile Generator");
        job.setJarByClass(ProductHFileDriver.class);

        // Configure job inputs and outputs
        job.setMapperClass(ProductHFileMapper.class);
        job.setReducerClass(ProductHFileReducer.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(org.apache.hadoop.hbase.KeyValue.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(org.apache.hadoop.hbase.KeyValue.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Configure HFileOutputFormat2 for HBase
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            TableName tableName = TableName.valueOf(args[2]);
            Admin admin = connection.getAdmin();

            // Fetch table descriptor
            TableDescriptor tableDescriptor = admin.getDescriptor(tableName);

            // Configure HFileOutputFormat2
            try (RegionLocator regionLocator = connection.getRegionLocator(tableName)) {
                HFileOutputFormat2.configureIncrementalLoad(job, tableDescriptor, regionLocator);
            }
        }

        // Submit the job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
