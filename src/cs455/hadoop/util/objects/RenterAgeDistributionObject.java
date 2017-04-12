package cs455.hadoop.util.objects;

import cs455.hadoop.util.writable.Run1CombinedWritable;

import java.util.ArrayList;

// Used to count the number of renters in each category
public class RenterAgeDistributionObject {
    // Used to count the number of renters in each category, 7 categories in total
    private long[] countArray = new long[7];

    public RenterAgeDistributionObject()
    {
        for (int i = 0; i < 7; i++)
        {
            countArray[i] = 0;
        }
    }

    // Perform deep copy in case of modification of input, input array must be in size 7, else perform empty constructor
    public RenterAgeDistributionObject(long[] countArray)
    {
        if (countArray.length == 7)
        {
            // Deep copy, can be replaced by System.arraycopy?
            for (int i = 0; i < 7; i++)
            {
                this.countArray[i] = countArray[i];
            }
        }
        else
        {
            // Empty constructor
            for (int i = 0; i < 7; i++)
            {
                countArray[i] = 0;
            }
        }
    }

    public static RenterAgeDistributionObject aggregate(ArrayList<Run1CombinedWritable> collection)
    {
        long[] countArray = new long[7];

        // Initialize with all 0
        for (int i = 0; i < 7; i++)
        {
            countArray[i] = 0;
        }

        // Iterate through the collection and add to countArray
        for (Run1CombinedWritable val: collection)
        {
            long[] tempCountArray = val.getRenterAgeDistributionObject().getCountArray();
            for (int i = 0; i < 7; i++)
            {
                countArray[i] += tempCountArray[i];
            }
        }

        return new RenterAgeDistributionObject(countArray);
    }

    public static RenterAgeDistributionObject aggregateByState(ArrayList<RenterAgeDistributionObject> collection)
    {
        long[] countArray = new long[7];

        // Initialize with all 0
        for (int i = 0; i < 7; i++)
        {
            countArray[i] = 0;
        }

        // Iterate through the collection and add to countArray
        for (RenterAgeDistributionObject val: collection)
        {
            long[] tempCountArray = val.getCountArray();
            for (int i = 0; i < 7; i++)
            {
                countArray[i] += tempCountArray[i];
            }
        }

        return new RenterAgeDistributionObject(countArray);
    }

    // Getter return deep copy of array for safety
    public long[] getCountArray()
    {
        long[] deepCopy = new long[7];
        for (int i = 0; i < 7; i++)
        {
            deepCopy[i] = countArray[i];
        }
        return deepCopy;
    }
}
