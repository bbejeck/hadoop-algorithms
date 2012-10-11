package bbejeck.mapred.aggregation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * User: Bill Bejeck
 * Date: 10/8/12
 * Time: 9:55 PM
 */
public class AverageTemperatureCombiningMapper extends Mapper<LongWritable, Text, Text, TemperatureAveragingPair> {

    private static final int MISSING = 9999;
    private Map<String,TemperatureAveragingPair> pairMap = new HashMap<String,TemperatureAveragingPair>();


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
            TemperatureAveragingPair pair = pairMap.get(yearMonth);
            if(pair == null){
                pair = new TemperatureAveragingPair();
                pairMap.put(yearMonth,pair);
            }
            int temps = pair.getTemp().get() + temp;
            int count = pair.getCount().get() + 1;
            pair.set(temps,count);
        }
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Set<String> keys = pairMap.keySet();
        Text keyText = new Text();
        for (String key : keys) {
             keyText.set(key);
             context.write(keyText,pairMap.get(key));
        }
    }
}
