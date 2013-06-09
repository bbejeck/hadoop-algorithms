package bbejeck.mapred.secondary;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Bill Bejeck
 * Date: 1/10/13
 * Time: 10:18 PM
 */
public class SecondarySortingTemperatureReducerTest {

    @Test
    public void testReducerCold() throws Exception {
        List<NullWritable> list = new ArrayList<NullWritable>();
        list.add(NullWritable.get());
        new ReduceDriver<TemperaturePair, NullWritable, Text, IntWritable>()
                .withReducer(new SecondarySortingTemperatureReducer())
                .withInput(new TemperaturePair("190101", -61), list)
                .withOutput(new Text("190101"), new IntWritable(-61))
                .runTest();

    }
}
