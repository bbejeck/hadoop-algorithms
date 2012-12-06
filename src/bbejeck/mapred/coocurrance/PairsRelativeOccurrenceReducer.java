package bbejeck.mapred.coocurrance;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * User: Bill Bejeck
 * Date: 11/27/12
 * Time: 10:33 PM
 */
public class PairsRelativeOccurrenceReducer extends Reducer<WordPair, IntWritable, WordPair, DoubleWritable> {
    private DoubleWritable totalCount = new DoubleWritable();
    private int totalWordCount = 1;
    private Text currentWord = new Text("NOT_SET");
    private Text flag = new Text("*");

    @Override
    protected void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        if (key.getNeighbor().equals(flag) && !key.getWord().equals(currentWord)) {
            currentWord = key.getWord();
            totalWordCount = getTotalCount(values);
        } else {
            int count = getTotalCount(values);
            totalCount.set((double)count/totalWordCount);
            context.write(key, totalCount);
        }

    }

    private int getTotalCount(Iterable<IntWritable> values) {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        return count;
    }
}
