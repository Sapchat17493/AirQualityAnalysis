package cs455.hadoop.mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * Mapper Code for finding SO2 levels for all years from hourly AirQuality Data provided by EPA
 * Link to Dataset(s): https://aqs.epa.gov/aqsweb/airdata/download_files.html
 * <p>
 * Author: Saptarshi Chatterjee
 * Contact: saptarshi.chatterjee@colostate.edu
 * Started on Mon Apr 6 20:00 2020 by Saptarshi Chatterjee
 * <p>
 * <p>
 * The generic inputs to the Mapper will be the offset and the line (LongWritable and Text respectively).
 * I will output, i.e, context-write from the map operation, the year along with the sum and count of measured for that
 * hour (Text and Text respectively).
 */

public class YearSO2TrendMapper extends Mapper<LongWritable, Text, Text, Text> {
    private HashMap<String, List<Double>> yearSO2;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        yearSO2 = new HashMap<>();
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

                    if (y >= 1980 && y <= 2019) {
                        double so2Level = Double.parseDouble(splitRecord.get(13));
                        updateYearSO2Map(year, so2Level);
                    }
                }
            }
        }
    }

    //Write Data in format <Text, Text>; <"year", "sum:count">
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, List<Double>> entry : yearSO2.entrySet()) {
            String year = entry.getKey();
            List<Double> list = entry.getValue();
            long count = list.size();
            double sum = sumList(list);
            System.out.println("Writing Key: " + year + " Val: " + String.valueOf(sum) + ":" + String.valueOf(count));
            context.write(new Text(year), new Text(String.valueOf(sum) + ":" + String.valueOf(count)));
        }
    }

    private void updateYearSO2Map(String year, double valToAdd) {
        if (yearSO2.containsKey(year)) {
            List<Double> list = yearSO2.get(year);
            list.add(valToAdd);
            yearSO2.put(year, list);
        } else {
            List<Double> list = new ArrayList<>();
            yearSO2.put(year, list);
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
