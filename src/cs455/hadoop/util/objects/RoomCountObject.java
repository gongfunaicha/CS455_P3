package cs455.hadoop.util.objects;

import cs455.hadoop.util.writable.Run1CombinedWritable;

import java.util.ArrayList;

// Used to count the number of houses by room
public class RoomCountObject {
    // Used to store the number of houses by number of rooms, 9 categories in total
    private long[] countArray = new long[9];

    public RoomCountObject()
    {
        for (int i = 0; i < 9; i++)
        {
            countArray[i] = 0;
        }
    }

    // Perform deep copy in case of modification of input, input array must be in size 9, else perform empty constructor
    public RoomCountObject(long[] countArray)
    {
        if (countArray.length == 9)
        {
            // Deep copy, can be replaced by System.arraycopy?
            for (int i = 0; i < 9; i++)
            {
                this.countArray[i] = countArray[i];
            }
        }
        else
        {
            // Empty constructor
            for (int i = 0; i < 9; i++)
            {
                countArray[i] = 0;
            }
        }
    }

    public static RoomCountObject aggregate(ArrayList<Run1CombinedWritable> collection)
    {
        long[] countArray = new long[9];

        // Initialize with all 0
        for (int i = 0; i < 9; i++)
        {
            countArray[i] = 0;
        }

        // Iterate through the collection and add to countArray
        for (Run1CombinedWritable val: collection)
        {
            long[] tempCountArray = val.getRoomCountObject().getCountArray();
            for (int i = 0; i < 9; i++)
            {
                countArray[i] += tempCountArray[i];
            }
        }

        return new RoomCountObject(countArray);
    }

    // Not used anymore
    public static RoomCountObject aggregateByState(ArrayList<RoomCountObject> collection)
    {
        long[] countArray = new long[9];

        // Initialize with all 0
        for (int i = 0; i < 9; i++)
        {
            countArray[i] = 0;
        }

        // Iterate through the collection and add to countArray
        for (RoomCountObject val: collection)
        {
            long[] tempCountArray = val.getCountArray();
            for (int i = 0; i < 9; i++)
            {
                countArray[i] += tempCountArray[i];
            }
        }

        return new RoomCountObject(countArray);
    }

    // Getter return deep copy of array for safety
    public long[] getCountArray()
    {
        long[] deepCopy = new long[9];
        for (int i = 0; i < 9; i++)
        {
            deepCopy[i] = countArray[i];
        }
        return deepCopy;
    }
}
