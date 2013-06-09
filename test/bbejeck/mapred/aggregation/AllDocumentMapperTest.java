package bbejeck.mapred.aggregation;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * User: Bill Bejeck
 * Date: 9/11/12
 * Time: 10:29 PM
 */
public class AllDocumentMapperTest {

   @Test
   public void testProcessWordCounts() throws Exception{
       Text text = new Text("foo bar bar baz bar foo");
       new MapDriver<LongWritable,Text,Text,IntWritable>()
               .withMapper(new AllDocumentMapper())
               .withInput(new LongWritable(1l),text)
               .withOutput(new Text("baz"),new IntWritable(1))
               .withOutput(new Text("foo"),new IntWritable(2))
               .withOutput(new Text("bar"),new IntWritable(3))
               .runTest();
   }

}
