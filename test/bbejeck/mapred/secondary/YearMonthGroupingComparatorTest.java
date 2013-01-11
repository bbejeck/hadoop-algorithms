package bbejeck.mapred.secondary;

import org.junit.Test;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

/**
 * User: Bill Bejeck
 * Date: 1/9/13
 * Time: 10:25 PM
 */
public class YearMonthGroupingComparatorTest {

    private TemperaturePair temperaturePair = new TemperaturePair("201203",50);
    private TemperaturePair temperaturePair2 = new TemperaturePair("201304",-10);
    private YearMonthGroupingComparator comparator = new YearMonthGroupingComparator();

    @Test
    public void testCompare() throws Exception {
       assertThat(comparator.compare(temperaturePair,temperaturePair2),is(-1));
    }

    @Test
    public void testCompareGreater() throws Exception {
        assertThat(comparator.compare(temperaturePair2,temperaturePair),is(1));
    }

    @Test
    public void testCompareEquals() throws Exception {
        assertThat(comparator.compare(temperaturePair2,temperaturePair2),is(0));
    }
}
