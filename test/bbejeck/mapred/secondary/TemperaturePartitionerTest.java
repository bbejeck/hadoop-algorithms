package bbejeck.mapred.secondary;

import org.apache.hadoop.io.NullWritable;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
/**
 * User: Bill Bejeck
 * Date: 1/9/13
 * Time: 10:17 PM
 */
public class TemperaturePartitionerTest {



    @Test
    public void testGetPartition() throws Exception {
        String yearMonth = "201301";
        int temperature = 15;
        TemperaturePair temperaturePair = new TemperaturePair();
        temperaturePair.setYearMonth(yearMonth);
        temperaturePair.setTemperature(temperature);
        int expectedPartition = temperaturePair.getYearMonth().hashCode() % 3;
        TemperaturePartitioner partitioner = new TemperaturePartitioner();
        int partition = partitioner.getPartition(temperaturePair, NullWritable.get(),3);
        assertThat(expectedPartition,is(partition));

    }
}
