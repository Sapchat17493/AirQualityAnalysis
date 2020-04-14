package cs455.hadoop.reducers;

import cs455.hadoop.helpers.StateTemp;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Reducer Code for finding top 10 hottest states in Summer from hourly AirQuality Data provided by EPA
 * Link to Dataset(s): https://aqs.epa.gov/aqsweb/airdata/download_files.html
 * <p>
 * Author: Saptarshi Chatterjee
 * Contact: saptarshi.chatterjee@colostate.edu
 * Started on Tue Apr 7 14:10 2020 by Saptarshi Chatterjee
 * <p>
 * <p>
 * The generic inputs to the Reducer will be the state name and a combination of sum and count of temperature and observations
 * for the summer months (Text and Text respectively). I will output, i.e, context-write from the reduce operation,
 * the top 10 hottest states for the summer months.
 */

public class Top10HottestReducer extends Reducer<Text, Text, Text, Text> {
    private List<StateTemp> orderedList = new ArrayList<>();

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
        double meanStateTemp = sums / (double) counts;

        StateTemp st = new StateTemp(k, meanStateTemp);
        orderedList.add(st);

        String res = "The average temp for the summer month in state " + k + " is " + String.valueOf(meanStateTemp);
        context.write(new Text(res), new Text(""));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Collections.sort(orderedList);
        context.write(new Text("\n\n\nNow printing top 10\n\n"), new Text(""));

        if (orderedList.size() >= 10) {
            for (int i = 0; i < 10; i++) {
                String res = "The average temp for the summer month in state " + orderedList.get(i).getState()
                        + " is " + String.valueOf(orderedList.get(i).getAvgTemp());
                context.write(new Text(res), new Text(""));
            }
        }
    }
}
