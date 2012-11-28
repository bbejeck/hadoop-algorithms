package bbejeck.mapred.coocurrance;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Set;

/**
 * User: Bill Bejeck
 * Date: 11/27/12
 * Time: 10:38 PM
 */
public class StripesReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
    private MapWritable incrementingMap = new MapWritable();

    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        incrementingMap.clear();
        for (MapWritable value : values) {
            addAll(value);
        }
        context.write(key, incrementingMap);
    }


    private void addAll(MapWritable mapWritable) {
        Set<Writable> keys = mapWritable.keySet();
        for (Writable key : keys) {
            IntWritable fromCount = (IntWritable) mapWritable.get(key);
            if (incrementingMap.containsKey(key)) {
                IntWritable count = (IntWritable) incrementingMap.get(key);
                count.set(count.get() + fromCount.get());
            } else {
                incrementingMap.put(key, fromCount);
            }
        }
    }
}
