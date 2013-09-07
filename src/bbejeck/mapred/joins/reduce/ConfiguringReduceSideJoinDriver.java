package bbejeck.mapred.joins.reduce;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * User: Bill Bejeck
 * Date: 6/11/13
 * Time: 9:27 PM
 */
public class ConfiguringReduceSideJoinDriver {


    public static void main(String[] args) throws Exception {
        Splitter splitter = Splitter.on('/');
        StringBuilder filePaths = new StringBuilder();
        InputStream inputStream = new FileInputStream(new File("./jobs/join-config.properties"));
        Properties properties = new Properties();
        properties.load(inputStream);
        Configuration config = new Configuration();
        StringBuilder builder = new StringBuilder();

        String defaultMapConfig = "keyIndex=0&separator=,";

        for (int i = 0; i < args.length - 1; i++) {
            String fileName = Iterables.getLast(splitter.split(args[i]));
            String mapConfig = properties.getProperty(fileName, defaultMapConfig);
            builder.append(mapConfig).append("&joinDelimiter=,&joinOrder=").append(i + 1);
            config.set(fileName, builder.toString());
            builder.setLength(0);
            filePaths.append(args[i]).append(",");
        }

        filePaths.setLength(filePaths.length() - 1);
        Job job = Job.getInstance(config, "ReduceSideJoin");
        job.setJarByClass(ConfiguringReduceSideJoinDriver.class);

        FileInputFormat.addInputPaths(job, filePaths.toString());
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));

        job.setMapperClass(ConfiguringJoiningMapper.class);
        job.setReducerClass(JoiningReducer.class);
        job.setPartitionerClass(TaggedJoiningPartitioner.class);
        job.setGroupingComparatorClass(TaggedJoiningGroupingComparator.class);
        job.setOutputKeyClass(TaggedKey.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
