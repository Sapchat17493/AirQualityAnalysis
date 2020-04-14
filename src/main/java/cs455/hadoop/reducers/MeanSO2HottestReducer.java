package cs455.hadoop.reducers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Reducer Code for finding the yearly SO2 levels for 10 hottest states from hourly AirQuality Data provided by EPA
 * Link to Dataset(s): https://aqs.epa.gov/aqsweb/airdata/download_files.html
 * <p>
 * Author: Saptarshi Chatterjee
 * Contact: saptarshi.chatterjee@colostate.edu
 * Started on Tue Apr 7 19:00 2020 by Saptarshi Chatterjee
 * <p>
 * <p>
 * The generic inputs to the Reducer will be the state name and year and a combination of sum and count of temperature and observations
 * for the summer months (Text and Text respectively). I will output, i.e, context-write from the reduce operation,
 * the yearly SO2 levels per state.
 */

public class MeanSO2HottestReducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, List<Double>> yearWiseMap = new HashMap<>();

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
        String state = k.split(":")[0];
        double meanYearSO2forState = sums / (double) counts;

        if (yearWiseMap.containsKey(state)) {
            List<Double> l = yearWiseMap.get(state);
            l.add(meanYearSO2forState);
            yearWiseMap.put(state, l);
        } else {
            List<Double> l = new ArrayList<>();
            l.add(meanYearSO2forState);
            yearWiseMap.put(state, l);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, List<Double>> entry : yearWiseMap.entrySet()) {
            List<Double> l = entry.getValue();
            double yearlyMean = sumList(l) / (double) l.size();
            String res = "The yearly SO2 mean (all 40 years) for the state " + entry.getKey() + " is " + yearlyMean;
            context.write(new Text(res), new Text(""));
        }
    }

    private double sumList(List<Double> l) {
        double sum = 0.0;
        for (double d : l) {
            sum += d;
        }
        return sum;
    }
}
