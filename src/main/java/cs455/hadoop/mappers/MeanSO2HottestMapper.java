package cs455.hadoop.mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * Mapper Code for finding the yearly SO2 levels for 10 hottest states from hourly AirQuality Data provided by EPA
 * Link to Dataset(s): https://aqs.epa.gov/aqsweb/airdata/download_files.html
 * <p>
 * Author: Saptarshi Chatterjee
 * Contact: saptarshi.chatterjee@colostate.edu
 * Started on Tue Apr 7 17:00 2020 by Saptarshi Chatterjee
 * <p>
 * <p>
 * The generic inputs to the Mapper will be the offset and the line (LongWritable and Text respectively).
 * I will output, i.e, context-write from the map operation, the state name and year along with the average temperature with
 * counts for the observations of summer(June, July, August) months
 */

public class MeanSO2HottestMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final String[] HOTTEST_STATES = {"nevada", "arizona", "louisiana", "mississippi", "arkansas",
            "florida", "texas", "puerto rico", "virgin islands", "georgia", "oklahoma"};
    private HashMap<String, List<Double>> yearlySO2Hottest;
    private List<String> hottest_states;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        yearlySO2Hottest = new HashMap<>();
        hottest_states = new ArrayList<>();

        hottest_states.addAll(Arrays.asList(HOTTEST_STATES));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String record = value.toString();
        List<String> splitRecord = new ArrayList<String>(Arrays.asList(record.split("\\s*,\\s*")));

        if (splitRecord.size() > 0 && splitRecord.size() == 24) {
            String lineType = splitRecord.get(0);
            lineType = lineType.substring(1, lineType.length() - 1);

            if (!lineType.equalsIgnoreCase("state code")) { //ignore header line
                String statename = splitRecord.get(21);

                if (!statename.isEmpty()) {
                    statename = statename.substring(1, (statename.length() - 1)).toLowerCase();
                    if (hottest_states.contains(statename)) {
                        String year = splitRecord.get(11);

                        if (!year.isEmpty() && year.length() == 12) {
                            year = year.substring(1, (year.length() - 1));
                            year = year.split("-")[0];

                            if (year.length() == 4) {
                                double so2Level = Double.parseDouble(splitRecord.get(13));
                                String toFind = statename + ":" + year;
                                updateYearlyStateSO2Map(toFind, so2Level);
                            }
                        }
                    }
                }
            }
        }
    }

    //Write Data in format <Text, Text>; <"state_name:year", "sum:count">
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, List<Double>> entry : yearlySO2Hottest.entrySet()) {
            String state = entry.getKey();
            List<Double> list = entry.getValue();
            long count = list.size();
            double sum = sumList(list);
            System.out.println("Writing Key: " + state + " Val: " + String.valueOf(sum) + ":" + String.valueOf(count));
            context.write(new Text(state), new Text(String.valueOf(sum) + ":" + String.valueOf(count)));
        }
    }

    private void updateYearlyStateSO2Map(String toFind, double valToAdd) {
        if (yearlySO2Hottest.containsKey(toFind)) {
            List<Double> list = yearlySO2Hottest.get(toFind);
            list.add(valToAdd);
            yearlySO2Hottest.put(toFind, list);
        } else {
            List<Double> list = new ArrayList<>();
            yearlySO2Hottest.put(toFind, list);
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
