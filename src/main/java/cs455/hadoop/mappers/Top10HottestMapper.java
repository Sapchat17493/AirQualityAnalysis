package cs455.hadoop.mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * Mapper Code for finding top 10 hottest states in Summer from hourly AirQuality Data provided by EPA
 * Link to Dataset(s): https://aqs.epa.gov/aqsweb/airdata/download_files.html
 * <p>
 * Author: Saptarshi Chatterjee
 * Contact: saptarshi.chatterjee@colostate.edu
 * Started on Tue Apr 7 13:00 2020 by Saptarshi Chatterjee
 * <p>
 * <p>
 * The generic inputs to the Mapper will be the offset and the line (LongWritable and Text respectively).
 * I will output, i.e, context-write from the map operation, the state name along with the average temperature with
 * counts for the observations of summer(June, July, August) months
 */

public class Top10HottestMapper extends Mapper<LongWritable, Text, Text, Text> {
    private HashMap<String, List<Double>> stateTemps;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        stateTemps = new HashMap<>();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String record = value.toString();
        List<String> splitRecord = new ArrayList<String>(Arrays.asList(record.split("\\s*,\\s*")));
        String statename = "";

        if (splitRecord.size() > 0 && splitRecord.size() == 24) {
            String lineType = splitRecord.get(0);
            lineType = lineType.substring(1, lineType.length() - 1);

            if (!lineType.equalsIgnoreCase("state code")) { //ignore header line
                statename = splitRecord.get(21);

                if (!statename.isEmpty()) {
                    statename = statename.substring(1, (statename.length() - 1));
                    String month = splitRecord.get(11);

                    if (!month.isEmpty() && month.length() == 12) {
                        month = month.substring(1, (month.length() - 1));
                        month = month.split("-")[1];
                        int m = Integer.parseInt(month);

                        if (m >= 6 && m <= 8) {
                            double temp = Double.parseDouble(splitRecord.get(13));
                            updateStateTempMap(statename, temp);
                        }
                    }
                }
            }
        }
    }

    //Write Data in format <Text, Text>; <"state_name", "sum:count">
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, List<Double>> entry : stateTemps.entrySet()) {
            String state = entry.getKey();
            List<Double> list = entry.getValue();
            long count = list.size();
            double sum = sumList(list);
            System.out.println("Writing Key: " + state + " Val: " + String.valueOf(sum) + ":" + String.valueOf(count));
            context.write(new Text(state), new Text(String.valueOf(sum) + ":" + String.valueOf(count)));
        }
    }

    private void updateStateTempMap(String state, double valToAdd) {
        if (stateTemps.containsKey(state)) {
            List<Double> list = stateTemps.get(state);
            list.add(valToAdd);
            stateTemps.put(state, list);
        } else {
            List<Double> list = new ArrayList<>();
            stateTemps.put(state, list);
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
