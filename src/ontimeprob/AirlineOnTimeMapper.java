package ontimeprob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import writables.LongX2Writable;

import java.io.IOException;

public class AirlineOnTimeMapper extends Mapper<LongWritable, Text, Text,
        LongX2Writable> {
    private LongX2Writable onTime = new LongX2Writable(1, 1);
    private LongX2Writable delayed = new LongX2Writable(0, 1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strings = value.toString().split(",");
        if ("Year".equals(strings[0])) {
            return;
        }
        value.set(strings[8]);
        if (strings[14].charAt(0) == '-') {
            context.write(value, onTime);
        } else {
            context.write(value, delayed);
        }
    }
}
