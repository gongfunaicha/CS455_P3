package cs455.hadoop.run1;

import cs455.hadoop.util.DataExtractor;
import cs455.hadoop.util.objects.*;
import cs455.hadoop.util.writable.Run1CombinedWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// Mapper class of run 1, solving question 1, 2, 3...
public class Run1Mapper extends Mapper<LongWritable, Text, Text, Run1CombinedWritable>{

    // Trying to extract as much data as
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Have null object for each util.objects
        ResidenceCountObject residenceCountObject = null;
        MarriageCountObject marriageCountObject = null;
        AgeDistributionObject ageDistributionObject = null;
        HousePositionCountObject housePositionCountObject = null;
        HouseValueCountObject houseValueCountObject = null;

        // Convert value into string
        String line = value.toString();

        // Index 9 - 10 is State Abbreviation
        String state = line.substring(8,10);

        // Index 11 - 13 is Summary level
        int summaryLevel = Integer.parseInt(line.substring(10,13));

        // If summary level not 100, skip
        if (summaryLevel != 100)
            return;

        // Index 25 - 28 is logical record part number
        int logicalRecordPartNumber = Integer.parseInt(line.substring(24,28));

        if (logicalRecordPartNumber == 1)
        {
            // Using part 1 we can solve question 2 and 3

            // First initialize other objects with empty constructor
            residenceCountObject = new ResidenceCountObject();
            housePositionCountObject = new HousePositionCountObject();
            houseValueCountObject = new HouseValueCountObject();

            // Extract data from line
            marriageCountObject = DataExtractor.marriageCountExtractor(line);
            ageDistributionObject = DataExtractor.ageDistributionExtractor(line);
        }
        else if (logicalRecordPartNumber == 2)
        {
            // Using part 2 we can solve question 1, 4, and 5

            // First initialize other objects with empty constructor
            marriageCountObject = new MarriageCountObject();
            ageDistributionObject = new AgeDistributionObject();

            // Extract data from line
            residenceCountObject = DataExtractor.residenceCountExtractor(line);
            housePositionCountObject = DataExtractor.housePositionCountExtractor(line);
            // TODO: Extract data for house value count object
        }
        else
        {
            // Invalid data, do not write anything
            return;
        }

        // Emit dummykey + Run1CombinedWritable pair
        context.write(new Text("1"), new Run1CombinedWritable(state, residenceCountObject, marriageCountObject, ageDistributionObject, housePositionCountObject, houseValueCountObject));

    }
}
