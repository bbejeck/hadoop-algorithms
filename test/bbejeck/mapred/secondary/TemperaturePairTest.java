package bbejeck.mapred.secondary;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

/**
 * User: Bill Bejeck
 * Date: 1/8/13
 * Time: 10:32 PM
 */
public class TemperaturePairTest {
    @Test
    public void testCompareTo() throws Exception {
        TemperaturePair temperaturePair = new TemperaturePair("201301",30);
        TemperaturePair temperaturePair1 = new TemperaturePair("201301",10);
        List<TemperaturePair> temperaturePairList = new ArrayList<TemperaturePair>();
        temperaturePairList.add(temperaturePair);
        temperaturePairList.add(temperaturePair1);
        Collections.sort(temperaturePairList);

        assertThat(temperaturePairList.get(0),is(temperaturePair1));
        assertThat(temperaturePairList.get(1),is(temperaturePair));
    }

    @Test
    public void testCompareToNegative() throws Exception {
        TemperaturePair temperaturePair = new TemperaturePair("201301",-30);
        TemperaturePair temperaturePair1 = new TemperaturePair("201301",-10);
        List<TemperaturePair> temperaturePairList = new ArrayList<TemperaturePair>();
        temperaturePairList.add(temperaturePair);
        temperaturePairList.add(temperaturePair1);
        Collections.sort(temperaturePairList);

        assertThat(temperaturePairList.get(0),is(temperaturePair));
        assertThat(temperaturePairList.get(1),is(temperaturePair1));
    }
}
