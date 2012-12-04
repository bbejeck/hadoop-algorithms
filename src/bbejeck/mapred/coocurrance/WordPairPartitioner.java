package bbejeck.mapred.coocurrance;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * User: Bill Bejeck
 * Date: 12/3/12
 * Time: 11:06 PM
 */
public class WordPairPartitioner extends Partitioner<WordPair,IntWritable> {

    @Override
    public int getPartition(WordPair wordPair, IntWritable intWritable, int numPartitions) {
        return wordPair.getWord().hashCode() % numPartitions;
    }
}
