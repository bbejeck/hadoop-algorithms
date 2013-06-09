package bbejeck.mapred.aggregation;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Bill Bejeck
 * Date: 10/9/12
 * Time: 11:18 PM
 */
public class AverageTemperatureCombinerTest {

    @Test
    public void testCombiner() throws Exception {
        List<TemperatureAveragingPair> pairList = new ArrayList<TemperatureAveragingPair>();
        pairList.add(new TemperatureAveragingPair(-78,1));
        pairList.add(new TemperatureAveragingPair(-84,1));
        pairList.add(new TemperatureAveragingPair(-28,1));
        pairList.add(new TemperatureAveragingPair(-56,1));
        new ReduceDriver<Text,TemperatureAveragingPair,Text,TemperatureAveragingPair>()
                .withReducer(new AverageTemperatureCombiner())
                .withInput(new Text("190101"), pairList)
                .withOutput(new Text("190101"),new TemperatureAveragingPair(-246,4))
                .runTest();

    }
}
