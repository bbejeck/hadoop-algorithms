package bbejeck.mapred.joins.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * User: Bill Bejeck
 * Date: 6/10/13
 * Time: 10:54 PM
 */
public class TaggedJoiningPartitioner extends Partitioner<TaggedKey,Text> {

    @Override
    public int getPartition(TaggedKey taggedKey, Text text, int numPartitions) {
        return taggedKey.getJoinKey().hashCode() % numPartitions;
    }
}
