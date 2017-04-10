package cs455.hadoop.run1;

import cs455.hadoop.util.objects.*;
import cs455.hadoop.util.writable.Run1CombinedWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

public class Run1Reducer extends Reducer<Text, Run1CombinedWritable, Text, Text>{

    @Override
    protected void reduce(Text key, Iterable<Run1CombinedWritable> values, Context context) throws IOException, InterruptedException {
        // Group Run1CombinedWritable by State
        // Used TreeMap due to it is sorted by key
        TreeMap<String, ArrayList<Run1CombinedWritable>> groupByState = new TreeMap<>();

        for (Run1CombinedWritable val: values)
        {
            String state = val.getState();
            if (!groupByState.containsKey(state))
            {
                // If haven't seen this key, add key and empty arraylist
                groupByState.put(state, new ArrayList<>());
            }

            // Deep copy val into corresponding ArrayList
            groupByState.get(state).add(new Run1CombinedWritable(val));
        }

        // Iterator through the groupByState, emit one Run1CombinedWritable for each state
        for (Map.Entry<String, ArrayList<Run1CombinedWritable>> entry : groupByState.entrySet())
        {
            String state = entry.getKey();
            ArrayList<Run1CombinedWritable> collection = entry.getValue();

            // Aggregate the counts
            ResidenceCountObject aggregatedResidenceCountObject = ResidenceCountObject.aggregate(collection);
            MarriageCountObject aggregatedMarriageCountObject = MarriageCountObject.aggregate(collection);
            AgeDistributionObject aggregatedAgeDistributionObject = AgeDistributionObject.aggregate(collection);
            HousePositionCountObject aggregatedHousePositionCountObject = HousePositionCountObject.aggregate(collection);
            HouseValueCountObject aggregatedHouseValueCountObject = HouseValueCountObject.aggregate(collection);


            // Perform analysis tasks
            ResidenceCountAnalysis(context, state, aggregatedResidenceCountObject);
            MarriageCountAnalysis(context, state, aggregatedMarriageCountObject);
            AgeDistributionAnalysis(context, state, aggregatedAgeDistributionObject);
            HousePositionCountAnalysis(context, state, aggregatedHousePositionCountObject);
            HouseValueCountAnalysis(context, state, aggregatedHouseValueCountObject);

        }
    }

    private void ResidenceCountAnalysis(Context context, String state, ResidenceCountObject residenceCountObject) throws IOException, InterruptedException
    {
        // Get rent count and owned count
        long rentCount = residenceCountObject.getRentCount();
        long ownedCount = residenceCountObject.getOwnedCount();

        // Get the rent and owned percentage
        double totalCount = rentCount + ownedCount;
        double rentPercentage;
        double ownedPercentage;
        if (totalCount == 0)
        {
            rentPercentage = 0;
            ownedPercentage = 0;
        }
        else
        {
            rentPercentage = (rentCount / totalCount) * 100;
            ownedPercentage = (ownedCount / totalCount) * 100;
        }
        // Convert into string
        String strRentPercentage = String.format("%.2f", rentPercentage) + "%";
        String strOwnedPercentage = String.format("%.2f", ownedPercentage) + "%";

        // Write to output
        context.write(new Text(state + " rented percentage: "), new Text(strRentPercentage));
        context.write(new Text(state + " owned percentage: "), new Text(strOwnedPercentage));

        // Debug output
        context.write(new Text(state + " rented count: "), new Text(String.valueOf(rentCount)));
        context.write(new Text(state + " owned count: "), new Text(String.valueOf(ownedCount)));
    }

    private void MarriageCountAnalysis(Context context, String state, MarriageCountObject marriageCountObject) throws IOException, InterruptedException
    {
        // Get never married count and total for both male and female
        long maleNeverMarried = marriageCountObject.getMaleNeverMarried();
        long maleTotal = marriageCountObject.getMaleTotal();
        long femaleNeverMarried = marriageCountObject.getFemaleNeverMarried();
        long femaleTotal = marriageCountObject.getFemaleTotal();

        // Get never married percentage for both male and female
        double maleNeverMarriedPercentage;
        double femaleNeverMarriedPercentage;
        if (maleTotal == 0)
        {
            maleNeverMarriedPercentage = 0;
        }
        else
        {
            maleNeverMarriedPercentage = (maleNeverMarried / (double)maleTotal) * 100;
        }
        if (femaleTotal == 0)
        {
            femaleNeverMarriedPercentage = 0;
        }
        else
        {
            femaleNeverMarriedPercentage = (femaleNeverMarried / (double)femaleTotal) * 100;
        }

        // Convert into string
        String strMaleNeverMarriedPercentage = String.format("%.2f", maleNeverMarriedPercentage) + "%";
        String strFemaleNeverMarriedPercentage = String.format("%.2f", femaleNeverMarriedPercentage) + "%";

        // Write to output
        context.write(new Text(state + " male never married percentage: "), new Text(strMaleNeverMarriedPercentage));
        context.write(new Text(state + " female never married percentage: "), new Text(strFemaleNeverMarriedPercentage));

        // Debug output
        context.write(new Text(state + " male never married count: "), new Text(String.valueOf(maleNeverMarried)));
        context.write(new Text(state + " total male population: "), new Text(String.valueOf(maleTotal)));
        context.write(new Text(state + " female never married count: "), new Text(String.valueOf(femaleNeverMarried)));
        context.write(new Text(state + " total female population: "), new Text(String.valueOf(femaleTotal)));
    }

    private void AgeDistributionAnalysis(Context context, String state, AgeDistributionObject ageDistributionObject) throws IOException, InterruptedException
    {
        // Get male and female count
        long male_18 = ageDistributionObject.getMale_18();
        long male19_29 = ageDistributionObject.getMale19_29();
        long male30_39 = ageDistributionObject.getMale30_39();
        long male40_ = ageDistributionObject.getMale40_();
        long female_18 = ageDistributionObject.getFemale_18();
        long female19_29 = ageDistributionObject.getFemale19_29();
        long female30_39 = ageDistributionObject.getFemale30_39();
        long female40_ = ageDistributionObject.getFemale40_();

        // Calculate total number of males
        double maleTotal = male_18 + male19_29 + male30_39 + male40_;
        double femaleTotal = female_18 + female19_29 + female30_39 + female40_;

        // Calculate percentage
        double male_18Percentage;
        double male19_29Percentage;
        double male30_39Percentage;
        double female_18Percentage;
        double female19_29Percentage;
        double female30_39Percentage;

        if (maleTotal == 0)
        {
            male_18Percentage = 0;
            male19_29Percentage = 0;
            male30_39Percentage = 0;
        }
        else
        {
            male_18Percentage = (male_18 / maleTotal) * 100;
            male19_29Percentage = (male19_29 / maleTotal) * 100;
            male30_39Percentage = (male30_39 / maleTotal) * 100;
        }
        if (femaleTotal == 0)
        {
            female_18Percentage = 0;
            female19_29Percentage = 0;
            female30_39Percentage = 0;
        }
        else
        {
            female_18Percentage = (female_18 / femaleTotal) * 100;
            female19_29Percentage = (female19_29 / femaleTotal) * 100;
            female30_39Percentage = (female30_39 / femaleTotal) * 100;
        }

        // Convert into string
        String stringMale_18Percentage = String.format("%.2f", male_18Percentage) + "%";
        String stringMale19_29Percentage = String.format("%.2f", male19_29Percentage) + "%";
        String stringMale30_39Percentage = String.format("%.2f", male30_39Percentage) + "%";
        String stringFemale_18Percentage = String.format("%.2f", female_18Percentage) + "%";
        String stringFemale19_29Percentage = String.format("%.2f", female19_29Percentage) + "%";
        String stringFemale30_39Percentage = String.format("%.2f", female30_39Percentage) + "%";


        // Write to output
        context.write(new Text(state + " Hispanic male below 18 years (inclusive) old percentage: "), new Text("\t\t\t" + stringMale_18Percentage));
        context.write(new Text(state + " Hispanic male between 19 (inclusive) and 29 (inclusive) years old percentage: "), new Text(stringMale19_29Percentage));
        context.write(new Text(state + " Hispanic male between 30 (inclusive) and 39 (inclusive) years old percentage: "), new Text(stringMale30_39Percentage));
        context.write(new Text(state + " Hispanic female below 18 years (inclusive) old percentage: "), new Text("\t\t" + stringFemale_18Percentage));
        context.write(new Text(state + " Hispanic female between 19 (inclusive) and 29 (inclusive) years old percentage: "), new Text(stringFemale19_29Percentage));
        context.write(new Text(state + " Hispanic female between 30 (inclusive) and 39 (inclusive) years old percentage: "), new Text(stringFemale30_39Percentage));

        // Debug output
        context.write(new Text(state + " Hispanic male below 18 years (inclusive) old count: "), new Text("\t\t\t" + String.valueOf(male_18)));
        context.write(new Text(state + " Hispanic male between 19 (inclusive) and 29 (inclusive) years old count: "), new Text(String.valueOf(male19_29)));
        context.write(new Text(state + " Hispanic male between 30 (inclusive) and 39 (inclusive) years old count: "), new Text(String.valueOf(male30_39)));
        context.write(new Text(state + " Hispanic male count: "), new Text("\t\t\t\t\t\t" + String.valueOf(maleTotal)));
        context.write(new Text(state + " Hispanic female below 18 years (inclusive) old count: "), new Text("\t\t" + String.valueOf(female_18)));
        context.write(new Text(state + " Hispanic female between 19 (inclusive) and 29 (inclusive) years old count: "), new Text(String.valueOf(female19_29)));
        context.write(new Text(state + " Hispanic female between 30 (inclusive) and 39 (inclusive) years old count: "), new Text(String.valueOf(female30_39)));
        context.write(new Text(state + " Hispanic female count: "), new Text("\t\t\t\t\t\t" + String.valueOf(femaleTotal)));
    }

    private void HousePositionCountAnalysis(Context context, String state, HousePositionCountObject housePositionCountObject) throws IOException, InterruptedException
    {
        // Get urban, rural, and other counts
        long urbanCount = housePositionCountObject.getUrbanCount();
        long ruralCount = housePositionCountObject.getRuralCount();
        long otherCount = housePositionCountObject.getOtherCount();

        // Calculate total number of households
        double totalCount = urbanCount + ruralCount + otherCount;

        // Calculate percentage
        double ruralPercentage;
        double urbanPercentage;
        if (totalCount == 0)
        {
            ruralPercentage = 0;
            urbanPercentage = 0;
        }
        else
        {
            ruralPercentage = (ruralCount / totalCount) * 100;
            urbanPercentage = (urbanCount / totalCount) * 100;
        }

        // Convert into String
        String stringRuralPercentage = String.format("%.2f", ruralPercentage) + "%";
        String stringUrbanPercentage = String.format("%.2f", urbanPercentage) + "%";

        // Write to output
        context.write(new Text(state + " rural household percentage: "), new Text(stringRuralPercentage));
        context.write(new Text(state + " urban household percentage: "), new Text(stringUrbanPercentage));

        // Debug output
        context.write(new Text(state + " rural household count: "), new Text(String.valueOf(ruralCount)));
        context.write(new Text(state + " urban household count: "), new Text(String.valueOf(urbanCount)));
        context.write(new Text(state + " other household count: "), new Text(String.valueOf(otherCount)));
        context.write(new Text(state + " total household count: "), new Text(String.valueOf(totalCount)));
    }

    private void HouseValueCountAnalysis(Context context, String state, HouseValueCountObject houseValueCountObject) throws IOException, InterruptedException
    {
        // Get house value count
        long[] valueArray = houseValueCountObject.getValueArray();

        // Calculate the total number of owners
        long totalCount = 0;
        for (int i = 0; i < 20; i++)
        {
            totalCount += valueArray[i];
        }

        // Handle the case that total count is 0
        if (totalCount == 0)
        {
            context.write(new Text(state), new Text(" does not contain any house occupied by owners."));
            return;
        }

        // Get the half-way count
        long halfWayCount = totalCount / 2;

        // Loop until current count is larger than halfWayCount
        long currentCount = 0;
        int i  = 0;
        for (i = 0; i < 20; i++)
        {
            if (currentCount > halfWayCount)
                break;
            currentCount += valueArray[i];
        }

        String medianHouseValue = "";
        // i cannot be 0
        switch (i)
        {
            case 1:
                medianHouseValue = "Less than $15,000";
                break;
            case 2:
                medianHouseValue = "$15,000 - $19,999";
                break;
            case 3:
                medianHouseValue = "$20,000 - $24,999";
                break;
            case 4:
                medianHouseValue = "$25,000 - $29,999";
                break;
            case 5:
                medianHouseValue = "$30,000 - $34,999";
                break;
            case 6:
                medianHouseValue = "$35,000 - $39,999";
                break;
            case 7:
                medianHouseValue = "$40,000 - $44,999";
                break;
            case 8:
                medianHouseValue = "$45,000 - $49,999";
                break;
            case 9:
                medianHouseValue = "$50,000 - $59,999";
                break;
            case 10:
                medianHouseValue = "$60,000 - $74,999";
                break;
            case 11:
                medianHouseValue = "$75,000 - $99,999";
                break;
            case 12:
                medianHouseValue = "$100,000 - $124,999";
                break;
            case 13:
                medianHouseValue = "$125,000 - $149,999";
                break;
            case 14:
                medianHouseValue = "$150,000 - $174,999";
                break;
            case 15:
                medianHouseValue = "$175,000 - $199,999";
                break;
            case 16:
                medianHouseValue = "$200,000 - $249,999";
                break;
            case 17:
                medianHouseValue = "$250,000 - $299,999";
                break;
            case 18:
                medianHouseValue = "$300,000 - $399,999";
                break;
            case 19:
                medianHouseValue = "$400,000 - $499,999";
                break;
            case 20:
                medianHouseValue = "$500,000 or more";
                break;
        }

        // Write to output
        context.write(new Text(state + " median value of the house that occupied by owners: "), new Text(medianHouseValue));

        // Debug output
        for (int j = 0; j < 20; j++)
        {
            String outputCate = "";
            switch (j + 1)
            {
                case 1:
                    outputCate = "Less than $15,000";
                    break;
                case 2:
                    outputCate = "$15,000 - $19,999";
                    break;
                case 3:
                    outputCate = "$20,000 - $24,999";
                    break;
                case 4:
                    outputCate = "$25,000 - $29,999";
                    break;
                case 5:
                    outputCate = "$30,000 - $34,999";
                    break;
                case 6:
                    outputCate = "$35,000 - $39,999";
                    break;
                case 7:
                    outputCate = "$40,000 - $44,999";
                    break;
                case 8:
                    outputCate = "$45,000 - $49,999";
                    break;
                case 9:
                    outputCate = "$50,000 - $59,999";
                    break;
                case 10:
                    outputCate = "$60,000 - $74,999";
                    break;
                case 11:
                    outputCate = "$75,000 - $99,999";
                    break;
                case 12:
                    outputCate = "$100,000 - $124,999";
                    break;
                case 13:
                    outputCate = "$125,000 - $149,999";
                    break;
                case 14:
                    outputCate = "$150,000 - $174,999";
                    break;
                case 15:
                    outputCate = "$175,000 - $199,999";
                    break;
                case 16:
                    outputCate = "$200,000 - $249,999";
                    break;
                case 17:
                    outputCate = "$250,000 - $299,999";
                    break;
                case 18:
                    outputCate = "$300,000 - $399,999";
                    break;
                case 19:
                    outputCate = "$400,000 - $499,999";
                    break;
                case 20:
                    outputCate = "$500,000 or more";
                    break;
            }
            context.write(new Text(state + " " + outputCate + " house values: "), new Text(String.valueOf(valueArray[j])));
        }

    }
}
