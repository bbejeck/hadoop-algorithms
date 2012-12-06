package bbejeck.mapred.coocurrance;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

/**
 * User: Bill Bejeck
 * Date: 12/5/12
 * Time: 10:36 PM
 */
public class PairsRelativeOccurrenceMapperTest {

    private IntWritable one = new IntWritable(1);
    private IntWritable two = new IntWritable(2);
    @Test
    public void testMapper(){
        new MapDriver<LongWritable, Text, WordPair, IntWritable>()
                .withMapper(new PairsRelativeOccurrenceMapper())
                .withInput(new LongWritable(1l),new Text("Ally Emmy Brady"))
                .withOutput(new WordPair("Ally","Emmy"), one)
                .withOutput(new WordPair("Ally","Brady"), one)
                .withOutput(new WordPair("Ally","*"),two)
                .withOutput(new WordPair("Emmy", "Ally"), one)
                .withOutput(new WordPair("Emmy","Brady"), one)
                .withOutput(new WordPair("Emmy","*"),two)
                .withOutput(new WordPair("Brady", "Ally"), one)
                .withOutput(new WordPair("Brady","Emmy"), one)
                .withOutput(new WordPair("Brady","*"),two)
                .runTest();
    }
}
