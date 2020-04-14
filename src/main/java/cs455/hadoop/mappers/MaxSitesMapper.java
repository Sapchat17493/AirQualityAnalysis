package cs455.hadoop.mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * Mapper Code for finding Maximum number of Sites per state from hourly AirQuality Data provided by EPA
 * Link to Dataset(s): https://aqs.epa.gov/aqsweb/airdata/download_files.html
 * <p>
 * Author: Saptarshi Chatterjee
 * Contact: saptarshi.chatterjee@colostate.edu
 * Started on Thu Apr 2 12:30 2020 by Saptarshi Chatterjee
 * <p>
 * <p>
 * The generic inputs to the Mapper will be the offset and the line (LongWritable and Text respectively).
 * I will output, i.e, context-write from the map operation, the State Name along with each unique site
 * for a state, where unique sites (air quality data monitoring sites) are denoted by a combination of
 * StateCode + CountyCode + SiteNum (Text and Text respectively). If we use cleanup we will simply
 * store the unique sites in a hashmap to be output from the cleanup method. I intend to have implementations
 * available for the combiner approach as well the cleanup approach and compare the effectiveness of both
 * approaches if time permits.
 */

public class MaxSitesMapper extends Mapper<LongWritable, Text, Text, Text> {
    private HashMap<String, String> uniqueSites;
    private Set<String> sites;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        uniqueSites = new HashMap<>();
        sites = new HashSet<>();
    }

    /**
     * @param key     The offset of the line in @param value
     * @param value   One line of the record contained in the input file
     * @param context The job context for writing intermediate outputs
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String record = value.toString();
        List<String> splitRecord = new ArrayList<String>(Arrays.asList(record.split("\\s*,\\s*")));
        String uniqueSiteID = "";
        String stateName = "", stateCode = "", countyCode = "", siteNum = "";

        if (splitRecord.size() > 0) {
            String lineType = splitRecord.get(0);
            lineType = lineType.substring(1, lineType.length() - 1);

            if (!lineType.equalsIgnoreCase("state code")) { //ignore header line
                if (splitRecord.size() >= 24) {
                    stateCode = splitRecord.get(0);
                    countyCode = splitRecord.get(1);
                    siteNum = splitRecord.get(2);

                    if (!stateCode.isEmpty() && !countyCode.isEmpty() && !siteNum.isEmpty()) {
                        stateCode = stateCode.substring(1, stateCode.length() - 1);
                        countyCode = countyCode.substring(1, countyCode.length() - 1);
                        siteNum = siteNum.substring(1, siteNum.length() - 1);
                        uniqueSiteID = stateCode + countyCode + siteNum; //create SiteID
                        stateName = splitRecord.get(21);

                        if (!uniqueSiteID.isEmpty() && uniqueSiteID.length() >= 9 && !sites.contains(uniqueSiteID) && !stateName.isEmpty()) { //unique sites only
                            stateName = stateName.substring(1, stateName.length() - 1);
                            sites.add(uniqueSiteID);
                            uniqueSites.put(uniqueSiteID, stateName);
                        }
                    }
                }
            }
        }

    }

    //Write Data in format <Text, Text>; <"state_name", "unique_site_ID">
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, String> entry : uniqueSites.entrySet()) {
            context.write(new Text(entry.getValue()), new Text(entry.getKey()));
        }
    }
}
