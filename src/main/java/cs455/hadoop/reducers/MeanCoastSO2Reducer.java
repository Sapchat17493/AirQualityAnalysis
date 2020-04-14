package cs455.hadoop.reducers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer Code for finding Higher Coastal SO2 levels from hourly AirQuality Data provided by EPA
 * Link to Dataset(s): https://aqs.epa.gov/aqsweb/airdata/download_files.html
 * <p>
 * Author: Saptarshi Chatterjee
 * Contact: saptarshi.chatterjee@colostate.edu
 * Started on Thu Apr 2 15:45 2020 by Saptarshi Chatterjee
 * <p>
 * <p>
 * The generic inputs to the Reducer will be the State Name and a combination of sum and count of SO2 levels for a state
 * (Text and Text respectively). I will output, i.e, context-write from the reduce operation, the final answer of which
 * coast produces what average level of SO2.
 */

public class MeanCoastSO2Reducer extends Reducer<Text, Text, Text, Text> {
    //private static final double EAST_COUNT = 16.0;
    //private static final double WEST_COUNT = 4.0;

    private double westSum = 0.0;
    private double eastSum = 0.0;
    private double eastCount = 0.0;
    private double westCount = 0.0;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long counts = 0;
        double sums = 0.0;

        for (Text t : values) {
            String val = t.toString();
            //System.out.println("Key: " + key.toString() + " Val: " + val);
            sums += Double.parseDouble(val.split(":")[0]);
            counts += Long.parseLong(val.split(":")[1]);
        }

        String k = key.toString();
        /*double meanOfState = sums / (double) counts;
        if (k.charAt(k.length() - 1) == 'W') {
            westSum += meanOfState;
        } else if (k.charAt(k.length() - 1) == 'E') {
            eastSum += meanOfState;
        }*/
        if (k.charAt(k.length() - 1) == 'W') {
            westSum += sums;
            westCount += counts;
        } else if (k.charAt(k.length() - 1) == 'E') {
            eastSum += sums;
            eastCount += counts;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        /*double meanWest = westSum / WEST_COUNT;
        double meanEast = eastSum / EAST_COUNT;
        String res = "The average of the West Coast states are " + String.valueOf(meanWest) + " ppb and the average " +
                "of the East Coast states are " + String.valueOf(meanEast) + " ppb!";*/
        double meanWest = westSum / westCount;
        double meanEast = eastSum / eastCount;
        String res = "The average SO2 levels of the West Coast states are " + String.valueOf(meanWest) + " ppb and the average " +
                "of the East Coast states are " + String.valueOf(meanEast) + " ppb!";
        context.write(new Text(res), new Text(""));
    }
}
