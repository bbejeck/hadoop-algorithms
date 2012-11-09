package bbejeck.mapred.aggregation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public class BasicMapRedDriver {
    private static final Logger log = Logger.getLogger(BasicMapRedDriver.class);

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(BasicMapRedDriver.class);
        if (args[0].equalsIgnoreCase("per")) {
            doMapReduce(job, args[1], PerTokenMapper.class, "per-token", "PerTokenCount", TokenCountReducer.class, IntWritable.class);
        } else if (args[0].equalsIgnoreCase("doc")) {
            doMapReduce(job, args[1], PerDocumentMapper.class, "per-document", "PerDocCount", TokenCountReducer.class, IntWritable.class);
        } else if (args[0].equalsIgnoreCase("all")) {
            doMapReduce(job, args[1], AllDocumentMapper.class, "all-document", "AllDocCount", TokenCountReducer.class, IntWritable.class);
        } else if (args[0].equalsIgnoreCase("comb")) {
            job.setCombinerClass(TokenCountReducer.class);
            doMapReduce(job, args[1], PerTokenMapper.class, "combiner", "CombinerCount", TokenCountReducer.class, IntWritable.class);
        } else if (args[0].equalsIgnoreCase("ave")) {
            doMapReduce(job, args[1], AverageTemperatureMapper.class, "simple-average", "SimpleAverage", AverageTemperatureReducer.class, TemperatureAveragingPair.class);
        } else if (args[0].equalsIgnoreCase("avcomb")) {
            job.setCombinerClass(AverageTemperatureCombiner.class);
            doMapReduce(job, args[1], AverageTemperatureMapper.class, "combiner-average", "CombinerAverage", AverageTemperatureReducer.class, TemperatureAveragingPair.class);
        } else if (args[0].equalsIgnoreCase("avinmap")) {
            doMapReduce(job, args[1], AverageTemperatureCombiningMapper.class, "inmapper-average", "InMapperAverage", AverageTemperatureReducer.class, TemperatureAveragingPair.class);
        }

    }


    private static void doMapReduce(Job job, String path, Class<? extends Mapper> mapperClass, String outPath, String jobName, Class<? extends Reducer> reducerClass, Class outputClass) throws Exception {
        try {
            job.setJobName(jobName);
            FileInputFormat.addInputPath(job, new Path(path));
            FileOutputFormat.setOutputPath(job, new Path(outPath));
            job.setMapperClass(mapperClass);
            job.setReducerClass(reducerClass);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(outputClass);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            log.error("Error running MapReduce Job",e);
        }
    }
}
