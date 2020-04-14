package cs455.hadoop.mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * Mapper Code for finding Hour with Max SO2 levels from hourly AirQuality Data provided by EPA
 * Link to Dataset(s): https://aqs.epa.gov/aqsweb/airdata/download_files.html
 * <p>
 * Author: Saptarshi Chatterjee
 * Contact: saptarshi.chatterjee@colostate.edu
 * Started on Sun Apr 5 22:45 2020 by Saptarshi Chatterjee
 * <p>
 * <p>
 * The generic inputs to the Mapper will be the offset and the line (LongWritable and Text respectively).
 * I will output, i.e, context-write from the map operation, the hour along with the sum and count of measured for that
 * hour (Text and Text respectively).
 */

public class MaxSO2HourMapper extends Mapper<LongWritable, Text, Text, Text> {
    private HashMap<String, List<Double>> hourSO2;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        hourSO2 = new HashMap<>();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String record = value.toString();
        List<String> splitRecord = new ArrayList<String>(Arrays.asList(record.split("\\s*,\\s*")));

        if (splitRecord.size() > 0 && splitRecord.size() == 24) {
            String lineType = splitRecord.get(0);
            lineType = lineType.substring(1, lineType.length() - 1);

            if (!lineType.equalsIgnoreCase("state code")) { //ignore header line
                String year = splitRecord.get(11);

                if (!year.isEmpty() && year.length() == 12) {
                    year = year.substring(1, (year.length() - 1));
                    year = year.split("-")[0];
                    int y = Integer.parseInt(year);

                    if (y >= 2000 && y <= 2019) { //Validating date
                        String hour = splitRecord.get(12);
                        if (!hour.isEmpty() && hour.length() == 7) {
                            hour = hour.substring(1, (year.length() - 1));
                            hour = hour.split(":")[0];
                            int h = Integer.parseInt(hour);

                            if (h >= 0 && h <= 23) {
                                double so2Level = Double.parseDouble(splitRecord.get(13));
                                updateHourSO2Map(hour, so2Level);
                            }
                        }
                    }
                }
            }
        }
    }

    //Write Data in format <Text, Text>; <"hour_number", "sum:count">
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, List<Double>> entry : hourSO2.entrySet()) {
            String hour = entry.getKey();
            List<Double> list = entry.getValue();
            long count = list.size();
            double sum = sumList(list);
            System.out.println("Writing Key: " + hour + " Val: " + String.valueOf(sum) + ":" + String.valueOf(count));
            context.write(new Text(hour), new Text(String.valueOf(sum) + ":" + String.valueOf(count)));
        }
    }

    private void updateHourSO2Map(String hour, double valToAdd) {
        if (hourSO2.containsKey(hour)) {
            List<Double> list = hourSO2.get(hour);
            list.add(valToAdd);
            hourSO2.put(hour, list);
        } else {
            List<Double> list = new ArrayList<>();
            hourSO2.put(hour, list);
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
