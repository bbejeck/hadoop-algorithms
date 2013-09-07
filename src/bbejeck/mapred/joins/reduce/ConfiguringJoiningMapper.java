package bbejeck.mapred.joins.reduce;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * User: Bill Bejeck
 * Date: 6/8/13
 * Time: 10:12 PM
 */
public class ConfiguringJoiningMapper extends Mapper<LongWritable, Text, TaggedKey, Text> {

    private int keyIndex;
    private Splitter splitter;
    private Joiner joiner;
    private TaggedKey taggedKey = new TaggedKey();
    private Text data = new Text();
    private int joinOrder;
    private Splitter.MapSplitter mapSplitter = Splitter.on("&").withKeyValueSeparator("=");

    protected void setup(Context context) throws IOException, InterruptedException {
        Map<String,String> configMap = getConfigurationMap(context);

        keyIndex = Integer.parseInt(configMap.get("keyIndex"));
        String separator = configMap.get("separator");
        splitter = Splitter.on(separator).trimResults();
        String joinDelimiter = configMap.get("joinDelimiter");
        joiner = Joiner.on(joinDelimiter);
        joinOrder = Integer.parseInt(configMap.get("joinOrder"));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        List<String> values = Lists.newArrayList(splitter.split(value.toString()));
        String joinKey = values.remove(keyIndex);
        String valuesWithOutKey = joiner.join(values);
        taggedKey.set(joinKey, joinOrder);
        data.set(valuesWithOutKey);
        context.write(taggedKey, data);
    }

    private Map<String,String> getConfigurationMap(Context context){
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String configString = context.getConfiguration().get(fileSplit.getPath().getName());
        return mapSplitter.split(configString);
    }

}
