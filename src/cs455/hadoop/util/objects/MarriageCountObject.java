package cs455.hadoop.util.objects;

import cs455.hadoop.util.writable.Run1CombinedWritable;

import java.util.ArrayList;

public class MarriageCountObject {
    private long maleNeverMarried;
    private long maleTotal;
    private long femaleNeverMarried;
    private long femaleTotal;

    public MarriageCountObject()
    {
        maleNeverMarried = 0;
        maleTotal = 0;
        femaleNeverMarried = 0;
        femaleTotal = 0;
    }

    public MarriageCountObject(long maleNeverMarried, long maleTotal, long femaleNeverMarried, long femaleTotal)
    {
        this.maleNeverMarried = maleNeverMarried;
        this.maleTotal = maleTotal;
        this.femaleNeverMarried = femaleNeverMarried;
        this.femaleTotal = femaleTotal;
    }

    // Method used to aggregate a list of collection
    public static MarriageCountObject aggregate(ArrayList<Run1CombinedWritable> collection)
    {
        long maleNeverMarried = 0;
        long maleTotal = 0;
        long femaleNeverMarried = 0;
        long femaleTotal = 0;

        for (Run1CombinedWritable val: collection)
        {
            MarriageCountObject marriageCountObject = val.getMarriageCountObject();
            maleNeverMarried += marriageCountObject.getMaleNeverMarried();
            maleTotal += marriageCountObject.getMaleTotal();
            femaleNeverMarried += marriageCountObject.getFemaleNeverMarried();
            femaleTotal += marriageCountObject.getFemaleTotal();
        }

        return new MarriageCountObject(maleNeverMarried, maleTotal, femaleNeverMarried, femaleTotal);
    }

    public long getMaleNeverMarried()
    {
        return maleNeverMarried;
    }

    public long getMaleTotal()
    {
        return maleTotal;
    }

    public long getFemaleNeverMarried()
    {
        return femaleNeverMarried;
    }

    public long getFemaleTotal()
    {
        return femaleTotal;
    }
}
