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
        long urbanCount = 0;
        long ruralCount = 0;
        long otherCount = 0;

        for (Run1CombinedWritable val: collection)
        {
            HousePositionCountObject housePositionCountObject = val.getHousePositionCountObject();
            urbanCount += housePositionCountObject.getUrbanCount();
            ruralCount += housePositionCountObject.getRuralCount();
            otherCount += housePositionCountObject.getOtherCount();
        }

        return new HousePositionCountObject(urbanCount, ruralCount, otherCount);
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
