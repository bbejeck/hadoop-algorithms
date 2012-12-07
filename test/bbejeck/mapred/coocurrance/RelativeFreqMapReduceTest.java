package bbejeck.mapred.coocurrance;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Test;

/**
 * User: Bill Bejeck
 * Date: 12/6/12
 * Time: 11:23 PM
 */
public class RelativeFreqMapReduceTest {
    private DoubleWritable half = new DoubleWritable(.5);

    @Test
    public void testMapReduce()throws Exception {

       new MapReduceDriver<LongWritable,Text,WordPair,IntWritable,WordPair,DoubleWritable>()
               .withMapper(new PairsRelativeOccurrenceMapper())
               .withInput(new LongWritable(1l),new Text("Ally Emmy Brady"))
               .withReducer(new PairsRelativeOccurrenceReducer())
               .withOutput(new WordPair("Ally","Brady"), half)
               .withOutput(new WordPair("Ally", "Emmy"), half)
               .withOutput(new WordPair("Brady", "Ally"), half)
               .withOutput(new WordPair("Brady","Emmy"), half)
               .withOutput(new WordPair("Emmy", "Ally"), half)
               .withOutput(new WordPair("Emmy","Brady"), half)
               .runTest();
    }
}
