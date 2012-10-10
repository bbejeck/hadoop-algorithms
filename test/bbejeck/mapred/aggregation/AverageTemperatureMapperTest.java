package bbejeck.mapred.aggregation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

/**
 * User: Bill Bejeck
 * Date: 10/9/12
 * Time: 9:35 PM
 */
public class AverageTemperatureMapperTest {

    String[] temps = new String[]{"0029029070999991901010106004+64333+023450FM-12+000599999V0202701N015919999999N0000001N9-00781+99999102001ADDGF108991999999999999999999",
            "0029029070999991901010113004+64333+023450FM-12+000599999V0202901N008219999999N0000001N9+00721+99999102001ADDGF104991999999999999999999",
            "0029029070999991901010120004+64333+023450FM-12+000599999V0209991C000019999999N0000001N9+99991+99999102001ADDGF108991999999999999999999",
            "0029029070999991901010206004+64333+023450FM-12+000599999V0201801N008219999999N0000001N9-00611+99999101831ADDGF108991999999999999999999",
            "0029029070999991901010213004+64333+023450FM-12+000599999V0201801N009819999999N0000001N9-00561+99999101761ADDGF108991999999999999999999",
            "0029029070999991901010220004+64333+023450FM-12+000599999V0201801N009819999999N0000001N9-00281+99999101751ADDGF108991999999999999999999",
            "0029029070999991901010306004+64333+023450FM-12+000599999V0202001N009819999999N0000001N9-00671+99999101701ADDGF106991999999999999999999",
            "0029029070999991901010313004+64333+023450FM-12+000599999V0202301N011819999999N0000001N9-00331+99999101741ADDGF108991999999999999999999",
            "0029029070999991901010320004+64333+023450FM-12+000599999V0202301N011819999999N0000001N9-00281+99999101741ADDGF108991999999999999999999",
            "0029029070999991901010406004+64333+023450FM-12+000599999V0209991C000019999999N0000001N9-00331+99999102311ADDGF108991999999999999999999"};

    @Test
    public void testMap() {
        new MapDriver<LongWritable, Text, Text, TemperatureAveragingPair>()
                .withMapper(new AverageTemperatureMapper())
                .withInput(new LongWritable(1), new Text(temps[0]))
                .withOutput(new Text("190101"), new TemperatureAveragingPair(-78, 1))
                .runTest();
    }

    @Test
    public void testMapLeadingPlus() {
        new MapDriver<LongWritable, Text, Text, TemperatureAveragingPair>()
                .withMapper(new AverageTemperatureMapper())
                .withInput(new LongWritable(1), new Text(temps[1]))
                .withOutput(new Text("190101"), new TemperatureAveragingPair(72, 1))
                .runTest();
    }

    @Test
    public void testMapNoValue() {
        new MapDriver<LongWritable, Text, Text, TemperatureAveragingPair>()
                .withMapper(new AverageTemperatureMapper())
                .withInput(new LongWritable(1), new Text(temps[2]))
                .runTest();
    }
}
