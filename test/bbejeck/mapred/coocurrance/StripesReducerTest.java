package bbejeck.mapred.coocurrance;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Bill Bejeck
 * Date: 11/27/12
 * Time: 10:55 PM
 */
public class StripesReducerTest {
    private IntWritable one = new IntWritable(1);
    private Text foo = new Text("foo");
    private Text bar = new Text("bar");
    @Test
    public void testReducer() throws Exception {
        MapWritable map = new MapWritable();
        map.put(foo,new IntWritable(2));
        map.put(bar,new IntWritable(2));


        MapWritable map1 = new MapWritable();
        map1.put(foo, new IntWritable(2));
        map1.put(bar, new IntWritable(2));


        List<MapWritable> mapList = new ArrayList<MapWritable>();
        mapList.add(map);
        mapList.add(map1);

        Text text = new Text("key");
        MapWritable resultMap = new MapWritable();
        resultMap.put(new Text("foo"),new IntWritable(4));
        resultMap.put(new Text("bar"),new IntWritable(4));


        new ReduceDriver<Text,MapWritable,Text,MapWritable>()
               .withReducer(new StripesReducer())
               .withInput(text,mapList)
               .withOutput(text,resultMap)
               .runTest();
    }
}
