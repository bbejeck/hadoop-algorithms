package bbejeck.mapred.aggregation;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * User: Bill Bejeck
 * Date: 10/9/12
 * Time: 10:27 PM
 */
public class AverageTemperatureReducer extends Reducer<Text, TemperatureAveragingPair, Text, IntWritable> {
    private IntWritable average = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<TemperatureAveragingPair> values, Context context) throws IOException, InterruptedException {
        int temp = 0;
        int count = 0;
        for (TemperatureAveragingPair pair : values) {
            temp += pair.getTemp().get();
            count += pair.getCount().get();
        }
        average.set(temp / count);
        context.write(key, average);
    }
}
