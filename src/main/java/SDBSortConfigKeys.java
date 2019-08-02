

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;


@Private
@Unstable
public enum SDBSortConfigKeys {

    NUM_ROWS("mapreduce.terasort.num-rows",
            "Number of rows to generate during teragen."),

    NUM_PARTITIONS("mapreduce.terasort.num.partitions",
            "Number of partitions used for sampling."),

    SAMPLE_SIZE("mapreduce.terasort.partitions.sample",
            "Sample size for each partition."),

    FINAL_SYNC_ATTRIBUTE("mapreduce.terasort.final.sync",
            "Perform a disk-persisting hsync at end of every file-write."),

    USE_TERA_SCHEDULER("mapreduce.terasort.use.terascheduler",
            "Use TeraScheduler for computing input split distribution."),

    USE_SIMPLE_PARTITIONER("mapreduce.terasort.simplepartitioner",
            "Use SimplePartitioner instead of TotalOrderPartitioner."),

    OUTPUT_REPLICATION("mapreduce.terasort.output.replication",
            "Replication factor to use for output data files.");

    private String confName;
    private String description;

    SDBSortConfigKeys(String configName, String description) {
        this.confName = configName;
        this.description = description;
    }

    public String key() {
        return this.confName;
    }

    public String toString() {
        return "<" + confName + ">     " + description;
    }

    public static final long DEFAULT_NUM_ROWS = 0L;
    public static final int DEFAULT_NUM_PARTITIONS = 10;
    public static final long DEFAULT_SAMPLE_SIZE = 100000L;
    public static final boolean DEFAULT_FINAL_SYNC_ATTRIBUTE = true;
    public static final boolean DEFAULT_USE_TERA_SCHEDULER = true;
    public static final boolean DEFAULT_USE_SIMPLE_PARTITIONER = false;
    public static final int DEFAULT_OUTPUT_REPLICATION = 1;
}
