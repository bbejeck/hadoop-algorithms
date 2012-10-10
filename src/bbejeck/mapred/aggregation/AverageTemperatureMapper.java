package bbejeck.mapred.aggregation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * User: Bill Bejeck
 * Date: 10/8/12
 * Time: 9:55 PM
 */
public class AverageTemperatureMapper extends Mapper<LongWritable, Text, Text, TemperatureAveragingPair> {

    private Text outText = new Text();
    private TemperatureAveragingPair pair = new TemperatureAveragingPair();
    private static final int MISSING = 9999;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String yearMonth = line.substring(15, 21);

        int tempStartPosition = 87;

        if (line.charAt(tempStartPosition) == '+') {
            tempStartPosition += 1;
        }

        int temp = Integer.parseInt(line.substring(tempStartPosition, 92));

        if (temp != MISSING) {
            outText.set(yearMonth);
            pair.set(temp, 1);
            context.write(outText, pair);
        }
    }
}
