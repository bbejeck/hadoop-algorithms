package bbejeck.mapred.coocurrance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * User: Bill Bejeck
 * Date: 12/11/12
 * Time: 10:17 PM
 */
public class RelativeFrequencyDriver {

    public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(RelativeFrequencyDriver.class);
        job.setJobName("Relative_Frequencies");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(PairsRelativeOccurrenceMapper.class);
        job.setReducerClass(PairsRelativeOccurrenceReducer.class);
        job.setCombinerClass(PairsReducer.class);
        job.setPartitionerClass(WordPairPartitioner.class);
        job.setNumReduceTasks(3);

        job.setOutputKeyClass(WordPair.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
