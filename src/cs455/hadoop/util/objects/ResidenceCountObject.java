package cs455.hadoop.util.objects;

import cs455.hadoop.util.writable.Run1CombinedWritable;

import java.util.ArrayList;

public class ResidenceCountObject {
    private long rentCount;
    private long ownedCount;

    public ResidenceCountObject() {
        rentCount = 0;
        ownedCount = 0;
    }

    public ResidenceCountObject(long rentCount, long ownedCount) {
        this.rentCount = rentCount;
        this.ownedCount = ownedCount;
    }

    // Method used to aggregate a list of collection
    public static ResidenceCountObject aggregate(ArrayList<Run1CombinedWritable> collection)
    {
        long rentCount = 0;
        long ownedCount = 0;

        for (Run1CombinedWritable val: collection)
        {
            ResidenceCountObject residenceCountObject = val.getResidenceCountObject();
            rentCount += residenceCountObject.getRentCount();
            ownedCount += residenceCountObject.getOwnedCount();
        }

        return new ResidenceCountObject(rentCount, ownedCount);
    }

    public long getRentCount() {
        return rentCount;
    }

    public long getOwnedCount() {
        return ownedCount;
    }
}
