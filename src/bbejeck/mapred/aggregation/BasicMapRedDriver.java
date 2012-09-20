package bbejeck.mapred.aggregation;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * User: Bill Bejeck
 * Date: 9/10/12
 * Time: 9:42 PM
 */
public class BasicMapRedDriver {

    public static void main(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(BasicMapRedDriver.class);
        if (args[0].equalsIgnoreCase("per")) {
            doMapReduce(job, args[1], PerTokenMapper.class, "per-token", "PerTokenCount");
        }else if(args[0].equalsIgnoreCase("doc")){
            doMapReduce(job,args[1],PerLineMapper.class,"per-document","PerDocCount");
        }else if(args[0].equalsIgnoreCase("all")){
            doMapReduce(job,args[1],AllDocumentMapper.class,"all-document","AllDocCount");
        }else if(args[0].equalsIgnoreCase("comb")){
            job.setCombinerClass(TokenCountReducer.class);
            doMapReduce(job,args[1],PerTokenMapper.class,"combiner","CombinerCount");
        }

    }



    private static void doMapReduce(Job job, String path, Class mapperClass, String outPath, String jobName) throws Exception {
        job.setJobName(jobName);
        FileInputFormat.addInputPath(job, new Path(path));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.setMapperClass(mapperClass);
        job.setReducerClass(TokenCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
