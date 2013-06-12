package bbejeck.mapred.joins.reduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * User: Bill Bejeck
 * Date: 6/10/13
 * Time: 11:00 PM
 */
public class TaggedJoiningGroupingComparator extends WritableComparator {


    public TaggedJoiningGroupingComparator() {
        super(TaggedKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TaggedKey taggedKey1 = (TaggedKey)a;
        TaggedKey taggedKey2 = (TaggedKey)b;
        return taggedKey1.getJoinKey().compareTo(taggedKey2.getJoinKey());
    }
}
