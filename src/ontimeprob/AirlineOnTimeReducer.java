package ontimeprob;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import writables.LongX2Writable;

import java.io.IOException;
import java.util.Objects;

public class AirlineOnTimeReducer extends Reducer<Text, LongX2Writable, Text,
        DoubleWritable> {

    private AirlineProbability[] top3 = new AirlineProbability[3];
    private AirlineProbability[] bottom3 = new AirlineProbability[3];

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        for (int i = 0; i < 3; i++) {
            top3[i] = new AirlineProbability(null, Double.MIN_VALUE);
            bottom3[i] = new AirlineProbability(null, Double.MAX_VALUE);
        }
    }

    @Override
    protected void reduce(Text key, Iterable<LongX2Writable> values,
                          Context context)
            throws IOException, InterruptedException {
        long a = 0, b = 0;
        for (LongX2Writable value : values) {
            a += value.getA();
            b += value.getB();
        }
        double prob = a / (double) b;
        if (top3[2].prob < prob) {
            top3[2].airline = key.toString();
            top3[2].prob = prob;

            for (int i = 1; i >= 0; i--) {
                if (top3[i].prob < top3[i + 1].prob) {
                    AirlineProbability temp = top3[i];
                    top3[i] = top3[i + 1];
                    top3[i + 1] = temp;
                } else {
                    break;
                }
            }
        }

        if (bottom3[2].prob > prob) {
            bottom3[2].airline = key.toString();
            bottom3[2].prob = prob;

            for (int i = 1; i >= 0; i--) {
                if (bottom3[i].prob > bottom3[i + 1].prob) {
                    AirlineProbability temp;
                    temp = bottom3[i];
                    bottom3[i] = bottom3[i + 1];
                    bottom3[i + 1] = temp;
                } else {
                    break;
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        Text key = new Text();
        DoubleWritable value = new DoubleWritable();

        for (int i = 0; i < 3; i++) {
            if (top3[i].airline == null) {
                continue;
            }
            key.set(top3[i].airline);
            value.set(top3[i].prob);
            context.write(key, value);
        }
        for (int i = 0; i < 3; i++) {
            if (bottom3[i].airline == null) {
                continue;
            }
            boolean skip = false;
            for (int j = 0; j < 3; j++) {
                if (Objects.equals(bottom3[i].airline, top3[j].airline)) {
                    skip = true;
                    break;
                }
            }
            if (skip) {
                continue;
            }
            key.set(bottom3[i].airline);
            value.set(bottom3[i].prob);
            context.write(key, value);
        }
    }

    private class AirlineProbability {
        String airline;
        double prob;

        AirlineProbability(String airline, double prob) {
            this.airline = airline;
            this.prob = prob;
        }

    }
}
