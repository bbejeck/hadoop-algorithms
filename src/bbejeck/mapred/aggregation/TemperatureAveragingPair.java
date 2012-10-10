package bbejeck.mapred.aggregation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User: Bill Bejeck
 * Date: 10/8/12
 * Time: 9:01 PM
 */
public class TemperatureAveragingPair implements Writable, WritableComparable<TemperatureAveragingPair> {

    private IntWritable temp;
    private IntWritable count;


    public TemperatureAveragingPair() {
        set(new IntWritable(0), new IntWritable(0));
    }


    public TemperatureAveragingPair(int temp, int count) {
        set(new IntWritable(temp), new IntWritable(count));
    }

    public void set(int temp, int count){
        this.temp.set(temp);
        this.count.set(count);
    }

    public void set(IntWritable temp, IntWritable count) {
        this.temp = temp;
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        temp.write(out);
        count.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        temp.readFields(in);
        count.readFields(in);
    }

    @Override
    public int compareTo(TemperatureAveragingPair other) {
        int compareVal = this.temp.compareTo(other.getTemp());
        if (compareVal != 0) {
            return compareVal;
        }
        return this.count.compareTo(other.getCount());
    }

    public static TemperatureAveragingPair read(DataInput in) throws IOException {
        TemperatureAveragingPair averagingPair = new TemperatureAveragingPair();
        averagingPair.readFields(in);
        return averagingPair;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TemperatureAveragingPair that = (TemperatureAveragingPair) o;

        if (!count.equals(that.count)) return false;
        if (!temp.equals(that.temp)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = temp.hashCode();
        result = 163 * result + count.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TemperatureAveragingPair{" +
                "temp=" + temp +
                ", count=" + count +
                '}';
    }

    public IntWritable getTemp() {
        return temp;
    }

    public IntWritable getCount() {
        return count;
    }
}
