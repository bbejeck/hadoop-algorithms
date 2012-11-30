package bbejeck.mapred.coocurrance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/**
 * User: Bill Bejeck
 * Date: 9/10/12
 * Time: 9:42 PM
 */
public class CoOccurrenceDriver {
    private static final Logger log = Logger.getLogger(CoOccurrenceDriver.class);

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(CoOccurrenceDriver.class);
        if (args[0].equalsIgnoreCase("pairs")) {
            job.setOutputKeyClass(WordPair.class);
            job.setOutputValueClass(IntWritable.class);
            doMapReduce(job, args[1], PairsOccurrenceMapper.class, "pairs", "pairs-co-occur", PairsReducer.class);
        } else if (args[0].equalsIgnoreCase("stripes")) {
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(MapWritable.class);
            doMapReduce(job, args[1], StripesOccurrenceMapper.class, "stripes", "stripes co-occur", StripesReducer.class);
        }

    }


    private static void doMapReduce(Job job, String path, Class<? extends Mapper> mapperClass, String outPath, String jobName, Class<? extends Reducer> reducerClass) throws Exception {
        try {
            job.setJobName(jobName);
            FileInputFormat.addInputPath(job, new Path(path));
            FileOutputFormat.setOutputPath(job, new Path(outPath));
            job.setMapperClass(mapperClass);
            job.setReducerClass(reducerClass);
            job.setCombinerClass(reducerClass);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            log.error("Error running MapReduce Job", e);
        }
    }
}
