package cs455.hadoop.util.objects;

import cs455.hadoop.util.writable.Run1CombinedWritable;

import java.util.ArrayList;

// Used to count the number of houses within each rent
public class RentCountObject {
    // Used to store count of houses within each value, 20 values intervals in total
    private long[] valueArray = new long[17];

    public RentCountObject()
    {
        for (int i = 0; i < 17; i++)
        {
            valueArray[i] = 0;
        }
    }

    // Perform deep copy in case of modification of input, input array must be in size 17, else perform empty constructor
    public RentCountObject(long[] valueArray)
    {
        if (valueArray.length == 17)
        {
            // Deep copy, can be replaced by System.arraycopy?
            for (int i = 0; i < 17; i++)
            {
                this.valueArray[i] = valueArray[i];
            }
        }
        else
        {
            // Empty constructor
            for (int i = 0; i < 17; i++)
            {
                valueArray[i] = 0;
            }
        }
    }

    public static RentCountObject aggregate(ArrayList<Run1CombinedWritable> collection)
    {
        long[] valueArray = new long[17];

        // Initialize with all 0
        for (int i = 0; i < 17; i++)
        {
            valueArray[i] = 0;
        }

        // Iterate through the collection and add to valueArray
        for (Run1CombinedWritable val: collection)
        {
            long[] tempValueArray = val.getRentCountObject().getValueArray();
            for (int i = 0; i < 17; i++)
            {
                valueArray[i] += tempValueArray[i];
            }
        }

        return new RentCountObject(valueArray);
    }

    // Getter return deep copy of array for safety
    public long[] getValueArray()
    {
        long[] deepCopy = new long[17];
        for (int i = 0; i < 17; i++)
        {
            deepCopy[i] = valueArray[i];
        }
        return deepCopy;
    }
}
