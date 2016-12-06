package cancellation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CancellationCodeReducer
        extends Reducer<Text, LongWritable, Text, LongWritable> {
    private String maxKey;
    private long maxValue = -1;

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        for (LongWritable value : values) {
            count += value.get();
        }
        if (maxValue < count) {
            maxValue = count;
            maxKey = key.toString();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (maxKey != null) {
            context.write(new Text(maxKey), new LongWritable(maxValue));
        }
    }
}
