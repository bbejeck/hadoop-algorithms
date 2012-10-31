package bbejeck.mapred.aggregation;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Test;

/**
 * User: Bill Bejeck
 * Date: 10/9/12
 * Time: 10:40 PM
 */
public class AverageTemperatureMapReduceTest {
    String[] temps = new String[]{"0029029070999991901010106004+64333+023450FM-12+000599999V0202701N015919999999N0000001N9-00781+99999102001ADDGF108991999999999999999999",
            "0029029070999991901010113004+64333+023450FM-12+000599999V0202901N008219999999N0000001N9+00721+99999102001ADDGF104991999999999999999999",
            "0029029070999991901010120004+64333+023450FM-12+000599999V0209991C000019999999N0000001N9+99991+99999102001ADDGF108991999999999999999999",
            "0029029070999991901010206004+64333+023450FM-12+000599999V0201801N008219999999N0000001N9-00611+99999101831ADDGF108991999999999999999999",
            "0029029070999991901010213004+64333+023450FM-12+000599999V0201801N009819999999N0000001N9-00561+99999101761ADDGF108991999999999999999999",
            "0029029070999991901010220004+64333+023450FM-12+000599999V0201801N009819999999N0000001N9-00281+99999101751ADDGF108991999999999999999999",
            "0029029070999991901020306004+64333+023450FM-12+000599999V0202001N009819999999N0000001N9-00671+99999101701ADDGF106991999999999999999999",
            "0029029070999991901020313004+64333+023450FM-12+000599999V0202301N011819999999N0000001N9-00331+99999101741ADDGF108991999999999999999999",
            "0029029070999991901020320004+64333+023450FM-12+000599999V0202301N011819999999N0000001N9-00281+99999101741ADDGF108991999999999999999999",
            "0029029070999991901020406004+64333+023450FM-12+000599999V0209991C000019999999N0000001N9-00331+99999102311ADDGF108991999999999999999999"};


    @Test
    public void testMapReduce() {

        new MapReduceDriver<LongWritable, Text, Text, TemperatureAveragingPair, Text, IntWritable>()
                .withMapper(new AverageTemperatureMapper())
                .withInput(new LongWritable(1), new Text(temps[0]))
                .withInput(new LongWritable(2), new Text(temps[1]))
                .withInput(new LongWritable(3), new Text(temps[2]))
                .withInput(new LongWritable(4), new Text(temps[3]))
                .withInput(new LongWritable(5), new Text(temps[6]))
                .withInput(new LongWritable(6), new Text(temps[7]))
                .withInput(new LongWritable(7), new Text(temps[8]))
                .withInput(new LongWritable(8), new Text(temps[9]))
                .withCombiner(new AverageTemperatureCombiner())
                .withReducer(new AverageTemperatureReducer())
                .withOutput(new Text("190101"), new IntWritable(-22))
                .withOutput(new Text("190102"), new IntWritable(-40))
                .runTest();
    }

    @Test
    public void testMapReduceNoCombiner() {

        new MapReduceDriver<LongWritable, Text, Text, TemperatureAveragingPair, Text, IntWritable>()
                .withMapper(new AverageTemperatureMapper())
                .withInput(new LongWritable(1), new Text(temps[9]))
                .withInput(new LongWritable(2), new Text(temps[1]))
                .withInput(new LongWritable(3), new Text(temps[8]))
                .withInput(new LongWritable(4), new Text(temps[3]))
                .withInput(new LongWritable(5), new Text(temps[6]))
                .withInput(new LongWritable(6), new Text(temps[7]))
                .withInput(new LongWritable(7), new Text(temps[2]))
                .withInput(new LongWritable(8), new Text(temps[0]))
                .withReducer(new AverageTemperatureReducer())
                .withOutput(new Text("190101"), new IntWritable(-22))
                .withOutput(new Text("190102"), new IntWritable(-40))
                .runTest();
    }
}
