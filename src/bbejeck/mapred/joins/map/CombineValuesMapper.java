package bbejeck.mapred.joins.map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

import java.io.IOException;

/**
 * User: Bill Bejeck
 * Date: 1/23/14
 * Time: 10:42 PM
 */
public class CombineValuesMapper extends Mapper<Text, TupleWritable, NullWritable, Text> {

    private static final NullWritable nullKey = NullWritable.get();
    private Text outValue = new Text();
    private StringBuilder valueBuilder = new StringBuilder();
    private String separator;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        separator = context.getConfiguration().get("separator");
    }

    @Override
    protected void map(Text key, TupleWritable value, Context context) throws IOException, InterruptedException {
        valueBuilder.append(key).append(separator);
        for (Writable writable : value) {
            valueBuilder.append(writable.toString()).append(separator);
        }
        valueBuilder.setLength(valueBuilder.length() - 1);
        outValue.set(valueBuilder.toString());
        context.write(nullKey, outValue);
        valueBuilder.setLength(0);
    }
}
