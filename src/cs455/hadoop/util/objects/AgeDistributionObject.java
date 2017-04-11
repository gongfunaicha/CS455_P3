package cs455.hadoop.util.objects;

import cs455.hadoop.util.writable.Run1CombinedWritable;

import java.util.ArrayList;

// Originally for Q3, reused for Q8
public class AgeDistributionObject {
    private long male_18;
    private long male19_29;
    private long male30_39;
    private long male40_84;
    private long male85_;
    private long female_18;
    private long female19_29;
    private long female30_39;
    private long female40_84;
    private long female85_;

    public AgeDistributionObject()
    {
        male_18 = 0;
        male19_29 = 0;
        male30_39 = 0;
        male40_84 = 0;
        male85_ = 0;
        female_18 = 0;
        female19_29 = 0;
        female30_39 = 0;
        female40_84 = 0;
        female85_ = 0;
    }

    public AgeDistributionObject(long male_18, long male19_29, long male30_39, long male40_84, long male85_, long female_18, long female19_29, long female30_39, long female40_84, long female85_)
    {
        this.male_18 = male_18;
        this.male19_29 = male19_29;
        this.male30_39 = male30_39;
        this.male40_84 = male40_84;
        this.male85_ = male85_;
        this.female_18 = female_18;
        this.female19_29 = female19_29;
        this.female30_39 = female30_39;
        this.female40_84 = female40_84;
        this.female85_ = female85_;
    }

    // Method used to aggregate a list of collection
    public static AgeDistributionObject aggregate(ArrayList<Run1CombinedWritable> collection)
    {
        long male_18 = 0;
        long male19_29 = 0;
        long male30_39 = 0;
        long male40_84 = 0;
        long male85_ = 0;
        long female_18 = 0;
        long female19_29 = 0;
        long female30_39 = 0;
        long female40_84 = 0;
        long female85_ = 0;

        for (Run1CombinedWritable val: collection)
        {
            AgeDistributionObject ageDistributionObject = val.getAgeDistributionObject();
            male_18 += ageDistributionObject.getMale_18();
            male19_29 += ageDistributionObject.getMale19_29();
            male30_39 += ageDistributionObject.getMale30_39();
            male40_84 += ageDistributionObject.getMale40_84();
            male85_ += ageDistributionObject.getMale85_();
            female_18 += ageDistributionObject.getFemale_18();
            female19_29 += ageDistributionObject.getFemale19_29();
            female30_39 += ageDistributionObject.getFemale30_39();
            female40_84 += ageDistributionObject.getFemale40_84();
            female85_ += ageDistributionObject.getFemale85_();
        }

        return new AgeDistributionObject(male_18, male19_29, male30_39, male40_84, male85_, female_18, female19_29, female30_39, female40_84, female85_);
    }

    public long getMale_18() {
        return male_18;
    }

    public long getMale19_29() {
        return male19_29;
    }

    public long getMale30_39() {
        return male30_39;
    }

    public long getMale40_84()
    {
        return male40_84;
    }

    public long getMale85_()
    {
        return male85_;
    }

    public long getFemale_18() {
        return female_18;
    }

    public long getFemale19_29() {
        return female19_29;
    }

    public long getFemale30_39() {
        return female30_39;
    }

    public long getFemale40_84()
    {
        return female40_84;
    }

    public long getFemale85_()
    {
        return female85_;
    }

}
