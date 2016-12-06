import cancellation.CancellationCodeMapper;
import cancellation.CancellationCodeReducer;
import ontimeprob.AirlineOnTimeMapper;
import ontimeprob.AirlineOnTimeReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import taxitime.TaxiTimeMapper;
import taxitime.TaxiTimeReducer;
import writables.LongX2SumReducer;
import writables.LongX2Writable;

import java.io.IOException;

public class AirlineStatistics {
    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        Path tempDir = new Path(args[0]);
        if (fileSystem.exists(tempDir)) {
            System.out.println("the Temp dir should not exist. "
                    + "Delete the temp dir and rerun");
            System.exit(1);
        }
        fileSystem.mkdirs(tempDir);
        Path inputPath = new Path(args[1]);


        Job onTimeJob = Job.getInstance(conf, "AirlineOnTimeProbability");
        onTimeJob.setJarByClass(AirlineStatistics.class);
        onTimeJob.setMapperClass(AirlineOnTimeMapper.class);
        onTimeJob.setCombinerClass(LongX2SumReducer.class);
        onTimeJob.setReducerClass(AirlineOnTimeReducer.class);
        onTimeJob.setMapOutputValueClass(LongX2Writable.class);
        onTimeJob.setOutputKeyClass(Text.class);
        onTimeJob.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.setInputPaths(onTimeJob, inputPath);
        FileOutputFormat.setOutputPath(onTimeJob, new Path(tempDir.toString()
                + "/AirlineOnTimeProbability"));
        onTimeJob.submit();

        Job cancelCodeJob = Job.getInstance(conf, "AirlineCancellationCode");
        cancelCodeJob.setJarByClass(AirlineStatistics.class);
        cancelCodeJob.setMapperClass(CancellationCodeMapper.class);
        cancelCodeJob.setCombinerClass(LongSumReducer.class);
        cancelCodeJob.setReducerClass(CancellationCodeReducer.class);
        cancelCodeJob.setOutputKeyClass(Text.class);
        cancelCodeJob.setOutputValueClass(LongWritable.class);
        FileInputFormat.setInputPaths(cancelCodeJob, inputPath);
        FileOutputFormat.setOutputPath(cancelCodeJob, new Path(
                tempDir.toString() + "/CancellationCode"));

        cancelCodeJob.submit();

        Job taxiTimeJob = Job.getInstance(conf, "AirlineTaxiTime");
        taxiTimeJob.setJarByClass(AirlineStatistics.class);
        taxiTimeJob.setMapperClass(TaxiTimeMapper.class);
        taxiTimeJob.setCombinerClass(LongX2SumReducer.class);
        taxiTimeJob.setReducerClass(TaxiTimeReducer.class);
        taxiTimeJob.setMapOutputValueClass(LongX2Writable.class);
        taxiTimeJob.setOutputKeyClass(Text.class);
        taxiTimeJob.setOutputValueClass(FloatWritable.class);
        FileInputFormat.setInputPaths(taxiTimeJob, inputPath);
        FileOutputFormat.setOutputPath(taxiTimeJob, new Path(tempDir.toString()
                + "/TaxiTime"));
        taxiTimeJob.submit();
        onTimeJob.waitForCompletion(true);
        cancelCodeJob.waitForCompletion(true);
        taxiTimeJob.waitForCompletion(true);
    }
}
