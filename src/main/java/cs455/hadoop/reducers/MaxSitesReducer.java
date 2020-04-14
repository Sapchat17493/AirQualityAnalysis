package cs455.hadoop.reducers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Reducer Code for finding Maximum number of Sites per state from hourly AirQuality Data provided by EPA
 * Link to Dataset(s): https://aqs.epa.gov/aqsweb/airdata/download_files.html
 * <p>
 * Author: Saptarshi Chatterjee
 * Contact: saptarshi.chatterjee@colostate.edu
 * Started on Thu Apr 2 13:10 2020 by Saptarshi Chatterjee
 * <p>
 * <p>
 * The generic inputs to the Reducer will be the State Name and ID of unique sites (Text and Text respectively).
 * I will output, i.e, context-write from the reduce operation, the maximum number of sites for the state,
 * where unique sites (air quality data monitoring sites) are denoted by a combination of
 * StateCode + CountyCode + SiteNum.
 */

public class MaxSitesReducer extends Reducer<Text, Text, Text, Text> {
    private String maxState = "";
    private long maxSites = Integer.MIN_VALUE;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> uniqueSites = new HashSet<>();

        for (Text t : values) { //loop to add sites to a set for uniqueness
            uniqueSites.add(t.toString());
        }

        if (uniqueSites.size() > maxSites) { //Update highest counts
            maxSites = uniqueSites.size();
            maxState = key.toString();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String output = "The state with maximum sites is " + maxState + " and the number of unique sites is " + maxSites;
        context.write(new Text(output), new Text(""));
    }
}
