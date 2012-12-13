package bbejeck.mapred.coocurrance;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * User: Bill Bejeck
 * Date: 12/5/12
 * Time: 11:10 PM
 */
public class PairsRelativeOccurrenceReducerTest {

    @Test
    public void testReduceStar() throws Exception {
        List<IntWritable> allCounts = new ArrayList<IntWritable>();
        allCounts.add(new IntWritable(4));
        PairsRelativeOccurrenceReducer reducer = new PairsRelativeOccurrenceReducer();
        new ReduceDriver<WordPair, IntWritable, WordPair, DoubleWritable>()
                .withReducer(reducer)
                .withInput(new WordPair("apple", "*"), allCounts)
                .runTest();
        Field f = reducer.getClass().getDeclaredField("totalCount");
        f.setAccessible(true);
        DoubleWritable dw = (DoubleWritable)f.get(reducer);
        assertThat(dw.get(), is((double)4));

        f = reducer.getClass().getDeclaredField("currentWord");
        f.setAccessible(true);
        assertThat((f.get(reducer)).toString(), is("apple"));


    }

    @Test
    public void testReduce() throws Exception {
        List<IntWritable> counts = new ArrayList<IntWritable>();
        counts.add(new IntWritable(2));
        PairsRelativeOccurrenceReducer reducer = new PairsRelativeOccurrenceReducer();
        Field f = reducer.getClass().getDeclaredField("totalCount");
        f.setAccessible(true);
        DoubleWritable dw = (DoubleWritable)f.get(reducer);
        dw.set(2);
        new ReduceDriver<WordPair, IntWritable, WordPair, DoubleWritable>()
                .withReducer(reducer)
                .withInput(new WordPair("apple", "nut"), counts)
                .withOutput(new WordPair("apple", "nut"), new DoubleWritable(1.0))
                .runTest();
    }
}
