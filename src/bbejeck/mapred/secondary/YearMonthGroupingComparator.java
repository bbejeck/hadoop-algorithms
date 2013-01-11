package bbejeck.mapred.secondary;

import org.apache.hadoop.io.Text;

import java.util.Comparator;

/**
 * User: Bill Bejeck
 * Date: 1/7/13
 * Time: 11:00 PM
 */
public class YearMonthGroupingComparator implements Comparator<TemperaturePair> {

    @Override
    public int compare(TemperaturePair temperaturePair, TemperaturePair temperaturePair2) {
        Text yearMonth1 = temperaturePair.getYearMonth();
        Text yearMonth2 = temperaturePair2.getYearMonth();
        return yearMonth1.compareTo(yearMonth2);
    }
}
