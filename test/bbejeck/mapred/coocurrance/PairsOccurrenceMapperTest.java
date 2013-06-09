package bbejeck.mapred.coocurrance;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

/**
 * User: Bill Bejeck
 * Date: 11/25/12
 * Time: 10:20 PM
 */
public class PairsOccurrenceMapperTest {

    private IntWritable one = new IntWritable(1);

    @Test
    public void testMapValidInput() throws Exception {
        String text1 = "The quick        brown                   fox           jumped over the lazy dog";
        new MapDriver<LongWritable,Text,WordPair,IntWritable>()
               .withMapper(new PairsOccurrenceMapper())
               .withInput(new LongWritable(1l), new Text(text1))
               .withOutput(wp("The", "quick"), one)
               .withOutput(wp("The","brown"),one)
               .withOutput(wp("quick","The"),one)
               .withOutput(wp("quick","brown"),one)
               .withOutput(wp("quick","fox"),one)
               .withOutput(wp("brown","The"),one)
               .withOutput(wp("brown","quick"),one)
               .withOutput(wp("brown","fox"),one)
               .withOutput(wp("brown","jumped"),one)
               .withOutput(wp("fox","quick"),one)
               .withOutput(wp("fox", "brown"), one)
               .withOutput(wp("fox","jumped"),one)
               .withOutput(wp("fox","over"),one)
               .withOutput(wp("jumped","brown"),one)
               .withOutput(wp("jumped","fox"),one)
               .withOutput(wp("jumped","over"),one)
               .withOutput(wp("jumped","the"),one)
               .withOutput(wp("over","fox"),one)
               .withOutput(wp("over","jumped"),one)
               .withOutput(wp("over","the"),one)
               .withOutput(wp("over","lazy"),one)
               .withOutput(wp("the","jumped"),one)
               .withOutput(wp("the","over"),one)
               .withOutput(wp("the","lazy"),one)
               .withOutput(wp("the","dog"),one)
               .withOutput(wp("lazy","over"),one)
               .withOutput(wp("lazy", "the"), one)
               .withOutput(wp("lazy","dog"),one)
               .withOutput(wp("dog","the"),one)
               .withOutput(wp("dog","lazy"),one)
               .runTest();
    }

    private WordPair wp(String word, String neighbor) {
        return new WordPair(word,neighbor);
    }


    @Test
    public void testOneTokenInput() throws Exception {
        new MapDriver<LongWritable,Text,WordPair,IntWritable>()
            .withMapper(new PairsOccurrenceMapper())
            .withInput(new LongWritable(1l),new Text("Foo    "))
            .runTest();
    }

    @Test
    public void testAllWhitespaceInput() throws Exception {
        new MapDriver<LongWritable,Text,WordPair,IntWritable>()
                .withMapper(new PairsOccurrenceMapper())
                .withInput(new LongWritable(1l),new Text("                  " +
                        "                   "))
                .runTest();
    }
}
