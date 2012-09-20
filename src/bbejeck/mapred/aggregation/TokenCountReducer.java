package bbejeck.mapred.aggregation;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * User: Bill Bejeck
 * Date: 9/10/12
 * Time: 9:39 PM
 */
public class TokenCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
              count+= value.get();
        }
        context.write(key,new IntWritable(count));
    }
}
