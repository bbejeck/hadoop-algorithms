package bbejeck.mapred.secondary;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * User: Bill Bejeck
 * Date: 12/3/12
 * Time: 11:06 PM
 */
public class TemperaturePartitioner extends Partitioner<TemperaturePair, NullWritable> {

    @Override
    public int getPartition(TemperaturePair temperaturePair, NullWritable nullWritable, int numPartitions) {
        return temperaturePair.getYearMonth().hashCode() % numPartitions;
    }
}
