package bbejeck.mapred.coocurrance;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Bill Bejeck
 * Date: 11/27/12
 * Time: 10:47 PM
 */
public class PairsReducerTest {

    private PairsReducer reducer = new PairsReducer();

    @Test
    public void testReducer(){
        List<IntWritable> counts = new ArrayList<IntWritable>();
        counts.add(new IntWritable(1));
        counts.add(new IntWritable(1));
        counts.add(new IntWritable(1));
        counts.add(new IntWritable(1));
        WordPair wordPair = new WordPair("foo","bar");
        new ReduceDriver<WordPair,IntWritable,WordPair,IntWritable>()
             .withReducer(new PairsReducer())
             .withInput(wordPair,counts)
             .withOutput(wordPair,new IntWritable(4))
             .runTest();
    }
}
