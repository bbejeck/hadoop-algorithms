package bbejeck.mapred.coocurrance;

import org.apache.hadoop.io.IntWritable;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
/**
 * User: Bill Bejeck
 * Date: 12/3/12
 * Time: 11:49 PM
 */
public class WordPairPartitionerTest {

    private int numberReducers = 4;
    private WordPair wordPair = new WordPair("foo","bar");
    private IntWritable count = new IntWritable(1);
    private WordPairPartitioner pairPartitioner = new WordPairPartitioner();

    @Test
    public void testGetPartition() throws Exception {
       int expectedPartition = wordPair.getWord().hashCode() % numberReducers;
       assertThat(pairPartitioner.getPartition(wordPair,count,numberReducers ),is(expectedPartition));
    }
}
