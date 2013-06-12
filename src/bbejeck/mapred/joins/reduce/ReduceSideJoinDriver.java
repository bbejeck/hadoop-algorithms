package bbejeck.mapred.joins.reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * User: Bill Bejeck
 * Date: 6/11/13
 * Time: 9:27 PM
 */
public class ReduceSideJoinDriver {


    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.set("keyIndex", "0");
        config.set("separator", ",");
        Job job = Job.getInstance(config, "ReduceSideJoin");
        job.setJarByClass(ReduceSideJoinDriver.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PrimaryJoiningMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SecondaryJoiningMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setReducerClass(JoiningReducer.class);
        job.setPartitionerClass(TaggedJoiningPartitioner.class);
        job.setGroupingComparatorClass(TaggedJoiningGroupingComparator.class);
        job.setOutputKeyClass(TaggedKey.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
