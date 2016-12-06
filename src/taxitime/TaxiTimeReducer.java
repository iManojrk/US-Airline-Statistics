package taxitime;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import writables.LongX2Writable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TaxiTimeReducer
        extends Reducer<Text, LongX2Writable, Text, FloatWritable> {
    private TaxiTime[] txInTop = new TaxiTime[3];
    private TaxiTime[] txInBot = new TaxiTime[3];
    private TaxiTime[] txOutTop = new TaxiTime[3];
    private TaxiTime[] txOutBot = new TaxiTime[3];

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        for (int i = 0; i < 3; i++) {
            txInTop[i] = new TaxiTime(null, Float.MIN_VALUE);
            txOutTop[i] = new TaxiTime(null, Float.MIN_VALUE);
            txInBot[i] = new TaxiTime(null, Float.MAX_VALUE);
            txOutBot[i] = new TaxiTime(null, Float.MAX_VALUE);
        }
    }

    @Override
    protected void reduce(Text key, Iterable<LongX2Writable> values, Context context) throws IOException, InterruptedException {
        long a = 0, b = 0;
        for (LongX2Writable value : values) {
            a += value.getA();
            b += value.getB();
        }
        float avg = a / (float) b;
        TaxiTime[] top, bot;
        String kStr = key.toString();
        if (kStr.charAt(kStr.length() - 1) == 'I') {
            top = txInTop;
            bot = txInBot;
        } else {
            top = txOutTop;
            bot = txOutBot;
        }

        if (top[0].time < avg) {
            top[0].time = avg;
            top[0].airport = kStr;

            for (int i = 1; i < top.length; i++) {
                if (top[i].time < top[i - 1].time) {
                    TaxiTime temp = top[i];
                    top[i] = top[i - 1];
                    top[i - 1] = temp;
                } else {
                    break;
                }
            }
        }

        if (bot[0].time > avg) {
            bot[0].time = avg;
            bot[0].airport = kStr;

            for (int i = 1; i < bot.length; i++) {
                if (bot[i].time > bot[i - 1].time) {
                    TaxiTime temp = bot[i];
                    bot[i] = bot[i - 1];
                    bot[i - 1] = temp;
                } else {
                    break;
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        HashMap<String, Float> output = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            output.putIfAbsent(txInTop[i].airport, txInTop[i].time);
            output.putIfAbsent(txInBot[i].airport, txInBot[i].time);
            output.putIfAbsent(txOutTop[i].airport, txOutTop[i].time);
            output.putIfAbsent(txOutBot[i].airport, txOutBot[i].time);
        }
        Text key = new Text();
        FloatWritable value = new FloatWritable();
        for (Map.Entry<String, Float> entry : output.entrySet()) {
            if (entry.getKey() != null) {
                key.set(entry.getKey());
                value.set(entry.getValue());
                context.write(key, value);
            }
        }
    }

    private class TaxiTime {
        String airport;
        float time;

        TaxiTime(String airport, float time) {
            this.airport = airport;
            this.time = time;
        }
    }
}
