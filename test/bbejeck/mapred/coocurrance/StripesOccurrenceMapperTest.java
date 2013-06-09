package bbejeck.mapred.coocurrance;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

/**
 * User: Bill Bejeck
 * Date: 11/26/12
 * Time: 10:14 PM
 */
public class StripesOccurrenceMapperTest {
      private LongWritable key = new LongWritable(1);
      private IntWritable one = new IntWritable(1);
      private IntWritable two = new IntWritable(2);
      private Text text = new Text("the boy said the");
      private MapWritable map = new MapWritable();
      private MapWritable map1 = new MapWritable();
      private MapWritable map2 = new MapWritable();
      private MapWritable map3 = new MapWritable();

    @Before
    public void setUp(){
      map.put(new Text("boy"),one);
      map.put(new Text("said"),one);

      map1.put(new Text("the"),two);
      map1.put(new Text("said"),one);

      map2.put(new Text("the"),two);
      map2.put(new Text("boy"),one);

      map3.put(new Text("boy"),one);
      map3.put(new Text("said"),one);

    }

    @Test
    public void testMap() throws Exception {
        new MapDriver<LongWritable,Text,Text,MapWritable>()
                .withMapper(new StripesOccurrenceMapper())
                .withInput(key,text)
                .withOutput(new Text("the"),map)
                .withOutput(new Text("boy"),map1)
                .withOutput(new Text("said"),map2)
                .withOutput(new Text("the"),map3)
                .runTest();
    }
}
