package taxitime;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import writables.LongX2Writable;

import java.io.IOException;

public class TaxiTimeMapper
        extends Mapper<LongWritable, Text, Text, LongX2Writable> {
    private LongX2Writable outValue = new LongX2Writable(0, 1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strings = value.toString().split(",");
        if ("Year".equals(strings[0])) {
            return;
        }

        if (!"NA".equals(strings[20])) {
            value.set(strings[16] + " O");
            outValue.setA(Long.parseLong(strings[20]));
            context.write(value, outValue);
        }

        if (!"NA".equals(strings[19])) {
            value.set(strings[17] + " I");
            outValue.setA(Long.parseLong(strings[19]));
            context.write(value, outValue);
        }
    }
}
