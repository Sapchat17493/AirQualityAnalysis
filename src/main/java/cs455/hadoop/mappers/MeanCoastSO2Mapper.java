package cs455.hadoop.mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * Mapper Code for finding Higher Coastal SO2 levels from hourly AirQuality Data provided by EPA
 * Link to Dataset(s): https://aqs.epa.gov/aqsweb/airdata/download_files.html
 * <p>
 * Author: Saptarshi Chatterjee
 * Contact: saptarshi.chatterjee@colostate.edu
 * Started on Thu Apr 2 15:00 2020 by Saptarshi Chatterjee
 * <p>
 * <p>
 * The generic inputs to the Mapper will be the offset and the line (LongWritable and Text respectively).
 * I will output, i.e, context-write from the map operation, the State Name along with the sum and count of measured SO2
 * (Text and Text respectively). I intend to have implementations available for the combiner approach as well the cleanup
 * approach and compare the effectiveness of both approaches if time permits.
 */

public class MeanCoastSO2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final String[] E_STATES = {"maine", "new hampshire", "massachusetts", "rhode island", "connecticut",
            "new york", "new jersey", "delaware", "maryland", "virginia", "north carolina", "south carolina",
            "georgia", "florida", "pennsylvania", "district of columbia"};
    private static final String[] W_STATES = {"california", "oregon", "washington", "alaska"};
    private List<String> e_states;
    private List<String> w_states;
    private HashMap<String, List<Double>> stateSO2;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        stateSO2 = new HashMap<>();
        w_states = new ArrayList<>();
        e_states = new ArrayList<>();
        w_states.addAll(Arrays.asList(W_STATES));
        e_states.addAll(Arrays.asList(E_STATES));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String record = value.toString();
        List<String> splitRecord = new ArrayList<String>(Arrays.asList(record.split("\\s*,\\s*")));

        if (splitRecord.size() > 0 && splitRecord.size() == 24) {
            String lineType = splitRecord.get(0);
            lineType = lineType.substring(1, lineType.length() - 1);

            if (!lineType.equalsIgnoreCase("state code")) { //ignore header line
                String statename = splitRecord.get(21).toLowerCase();

                if (!statename.isEmpty()) {
                    statename = statename.substring(1, (statename.length() - 1));

                    if (e_states.contains(statename) || w_states.contains(statename)) {
                        double measuredSO2 = Double.parseDouble(splitRecord.get(13));

                        if (e_states.contains(statename)) {
                            String toFind = statename + "E";
                            updateStateSO2Map(toFind, measuredSO2);
                        } else if (w_states.contains(statename)) {
                            String toFind = statename + "W";
                            updateStateSO2Map(toFind, measuredSO2);
                        }
                    }
                }
            }
        }
    }

    //Write Data in format <Text, Text>; <"state_name", "sum:count">
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, List<Double>> entry : stateSO2.entrySet()) {
            String state = entry.getKey();
            List<Double> list = entry.getValue();
            long count = list.size();
            double sum = sumList(list);
            System.out.println("Writing Key: " + state + " Val: " + String.valueOf(sum) + ":" + String.valueOf(count));
            context.write(new Text(state), new Text(String.valueOf(sum) + ":" + String.valueOf(count)));
        }
    }

    private void updateStateSO2Map(String toFind, double valToAdd) {
        if (stateSO2.containsKey(toFind)) {
            List<Double> list = stateSO2.get(toFind);
            list.add(valToAdd);
            stateSO2.put(toFind, list);
        } else {
            List<Double> list = new ArrayList<>();
            stateSO2.put(toFind, list);
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
