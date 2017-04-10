package cs455.hadoop.util.objects;

import cs455.hadoop.util.writable.Run1CombinedWritable;

import java.util.ArrayList;

// Used to store the rural, urban, and other house count
public class HousePositionCountObject {
    private long urbanCount;
    private long ruralCount;
    private long otherCount;

    public HousePositionCountObject() {
        urbanCount = 0;
        ruralCount = 0;
        otherCount = 0;
    }

    // Method used to aggregate a list of collection
    public HousePositionCountObject(long urbanCount, long ruralCount, long otherCount) {
        this.urbanCount = urbanCount;
        this.ruralCount = ruralCount;
        this.otherCount = otherCount;
    }

    public static HousePositionCountObject aggregate(ArrayList<Run1CombinedWritable> collection)
    {
        // TODO: Implement aggregate method
        return null;
    }

    public long getUrbanCount()
    {
        return urbanCount;
    }

    public long getRuralCount()
    {
        return ruralCount;
    }

    public long getOtherCount()
    {
        return otherCount;
    }
}
