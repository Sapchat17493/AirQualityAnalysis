package cs455.hadoop.jobs;

import cs455.hadoop.mappers.YearSO2TrendMapper;
import cs455.hadoop.reducers.YearSO2TrendReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class YearSO2TrendJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SO2_Trend_Year");
        job.setJarByClass(YearSO2TrendJob.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(YearSO2TrendMapper.class);
        job.setReducerClass(YearSO2TrendReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        //job.setNumReduceTasks(10);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
