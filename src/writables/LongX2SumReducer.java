package writables;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LongX2SumReducer<KEY extends WritableComparable<KEY>>
        extends Reducer<KEY, LongX2Writable, KEY, LongX2Writable> {
    private LongX2Writable combinedValue = new LongX2Writable();

    @Override
    protected void reduce(KEY key, Iterable<LongX2Writable> values,
                          Context context)
            throws IOException, InterruptedException {
        long a = 0, b = 0;
        for (LongX2Writable value : values) {
            a += value.getA();
            b += value.getB();
        }
        combinedValue.setA(a);
        combinedValue.setB(b);
        context.write(key, combinedValue);
    }
}
