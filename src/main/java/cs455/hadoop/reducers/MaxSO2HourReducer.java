package cs455.hadoop.reducers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer Code for finding Hour with Max SO2 levels from hourly AirQuality Data provided by EPA
 * Link to Dataset(s): https://aqs.epa.gov/aqsweb/airdata/download_files.html
 * <p>
 * Author: Saptarshi Chatterjee
 * Contact: saptarshi.chatterjee@colostate.edu
 * Started on Sun Apr 5 23:55 2020 by Saptarshi Chatterjee
 * <p>
 * <p>
 * The generic inputs to the Reducer will be the hour number and a combination of sum and count of SO2 levels for that hour
 * (Text and Text respectively). I will output, i.e, context-write from the reduce operation, the final answer of which
 * hour has the highest average level of SO2 along with all other hourly values as well.
 */

public class MaxSO2HourReducer extends Reducer<Text, Text, Text, Text> {
    private String highestHour = "";
    private double highestSO2Level = Double.MIN_VALUE;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long counts = 0;
        double sums = 0.0;

        for (Text t : values) {
            String val = t.toString();
            System.out.println("Key: " + key.toString() + " Val: " + val);
            sums += Double.parseDouble(val.split(":")[0]);
            counts += Long.parseLong(val.split(":")[1]);
        }
        String k = key.toString();
        double meanHourSO2 = sums / (double) counts;

        if (meanHourSO2 > highestSO2Level) {
            highestHour = k;
            highestSO2Level = meanHourSO2;
        }

        String res = "The average SO2 for the hour " + k + "th hour of each day is " + String.valueOf(meanHourSO2);
        context.write(new Text(res), new Text(""));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String res = "The highest average SO2 for a given hour of day between 2000 - 2019 is for hour number "
                + highestHour + " of the day and the mean SO2 level for that hour is " + String.valueOf(highestSO2Level);
        context.write(new Text(res), new Text(""));
    }
}
