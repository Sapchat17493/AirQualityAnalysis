package cs455.hadoop.reducers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer Code for finding SO2 levels for all years from hourly AirQuality Data provided by EPA
 * Link to Dataset(s): https://aqs.epa.gov/aqsweb/airdata/download_files.html
 * <p>
 * Author: Saptarshi Chatterjee
 * Contact: saptarshi.chatterjee@colostate.edu
 * Started on Mon Apr 6 22:00 2020 by Saptarshi Chatterjee
 * <p>
 * <p>
 * The generic inputs to the Reducer will be the year number and a combination of sum and count of SO2 levels for that year
 * (Text and Text respectively). I will output, i.e, context-write from the reduce operation, the final answer which will
 * contain averages of all years
 */

public class YearSO2TrendReducer extends Reducer<Text, Text, Text, Text> {

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
        double meanYearSO2 = sums / (double) counts;

        String res = "The average SO2 for the year " + k + " is " + String.valueOf(meanYearSO2);
        context.write(new Text(res), new Text(""));
    }
}
