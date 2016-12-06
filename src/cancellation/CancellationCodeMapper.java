package cancellation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CancellationCodeMapper
        extends Mapper<LongWritable, Text, Text, LongWritable> {
    private LongWritable one = new LongWritable(1);
    private Text outKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if ("Year".equals(fields[0]) || "0".equals(fields[21])) {
            return;
        }
        outKey.set(fields[22]);
        context.write(outKey, one);
    }
}
