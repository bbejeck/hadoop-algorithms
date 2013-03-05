package bbejeck.mapred.secondary;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * User: Bill Bejeck
 * Date: 10/9/12
 * Time: 10:27 PM
 */
public class SecondarySortingTemperatureReducer extends Reducer<TemperaturePair, NullWritable, Text, IntWritable> {

    @Override
    protected void reduce(TemperaturePair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key.getYearMonth(), key.getTemperature());
    }
}
