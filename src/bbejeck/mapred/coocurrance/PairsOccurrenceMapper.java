package bbejeck.mapred.coocurrance;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * User: Bill Bejeck
 * Date: 11/24/12
 * Time: 1:31 AM
 */
public class PairsOccurrenceMapper extends Mapper<LongWritable, Text, WordPair, IntWritable> {
    private WordPair wordPair = new WordPair();
    private IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int numberNeighbors = context.getConfiguration().getInt("neighbors", 2);
        String[] tokens = value.toString().split("\\s+");
        if (tokens.length > 1) {
            for (int i = 0; i < tokens.length; i++) {
                wordPair.setWord(tokens[i]);

                int start = (i - numberNeighbors < 0) ? 0 : i - numberNeighbors;
                int end = (i + numberNeighbors >= tokens.length) ? tokens.length - 1 : i + numberNeighbors;
                for (int j = start; j <= end; j++) {
                    if (j == i) continue;
                    wordPair.setNeighbor(tokens[j]);
                    context.write(wordPair, ONE);
                }
            }
        }
    }
}
