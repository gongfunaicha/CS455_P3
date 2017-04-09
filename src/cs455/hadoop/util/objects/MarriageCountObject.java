package cs455.hadoop.util.objects;

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
