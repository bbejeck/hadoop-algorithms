package bbejeck.mapred.secondary;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User: Bill Bejeck
 * Date: 1/7/13
 * Time: 11:03 PM
 */
public class TemperaturePair implements Writable, WritableComparable<TemperaturePair> {

    private Text yearMonth = new Text();
    private IntWritable temperature = new IntWritable();


    public TemperaturePair() {
    }

    public TemperaturePair(String ym, int temp) {
        yearMonth.set(ym);
        temperature.set(temp);
    }

    public static TemperaturePair read(DataInput in) throws IOException {
        TemperaturePair temperaturePair = new TemperaturePair();
        temperaturePair.readFields(in);
        return temperaturePair;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        yearMonth.write(out);
        temperature.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        yearMonth.readFields(in);
        temperature.readFields(in);
    }

    @Override
    public int compareTo(TemperaturePair temperaturePair) {
        int compareValue = this.yearMonth.compareTo(temperaturePair.getYearMonth());
        if (compareValue == 0) {
            compareValue = temperature.compareTo(temperaturePair.getTemperature());
        }
        return compareValue;
    }

    public Text getYearMonth() {
        return yearMonth;
    }

    public IntWritable getTemperature() {
        return temperature;
    }

    public void setYearMonth(String yearMonthStr) {
        yearMonth.set(yearMonthStr);
    }

    public void setTemperature(int temp) {
        temperature.set(temp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TemperaturePair that = (TemperaturePair) o;

        if (temperature != null ? !temperature.equals(that.temperature) : that.temperature != null) return false;
        if (yearMonth != null ? !yearMonth.equals(that.yearMonth) : that.yearMonth != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = yearMonth != null ? yearMonth.hashCode() : 0;
        result = 31 * result + (temperature != null ? temperature.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TemperaturePair{" +
                "yearMonth=" + yearMonth +
                ", temperature=" + temperature +
                '}';
    }
}
