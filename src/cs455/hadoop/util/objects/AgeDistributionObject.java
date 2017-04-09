package cs455.hadoop.util.objects;

import cs455.hadoop.util.writable.Run1CombinedWritable;

import java.util.ArrayList;

public class AgeDistributionObject {
    private long male_18;
    private long male19_29;
    private long male30_39;
    private long male40_;
    private long female_18;
    private long female19_29;
    private long female30_39;
    private long female40_;

    public AgeDistributionObject()
    {
        male_18 = 0;
        male19_29 = 0;
        male30_39 = 0;
        male40_ = 0;
        female_18 = 0;
        female19_29 = 0;
        female30_39 = 0;
        female40_ = 0;
    }

    public AgeDistributionObject(long male_18, long male19_29, long male30_39, long male40_, long female_18, long female19_29, long female30_39, long female40_)
    {
        this.male_18 = male_18;
        this.male19_29 = male19_29;
        this.male30_39 = male30_39;
        this.male40_ = male40_;
        this.female_18 = female_18;
        this.female19_29 = female19_29;
        this.female30_39 = female30_39;
        this.female40_ = female40_;
    }

    // Method used to aggregate a list of collection
    public static AgeDistributionObject aggregate(ArrayList<Run1CombinedWritable> collection)
    {
        long male_18 = 0;
        long male19_29 = 0;
        long male30_39 = 0;
        long male40_ = 0;
        long female_18 = 0;
        long female19_29 = 0;
        long female30_39 = 0;
        long female40_ = 0;

        for (Run1CombinedWritable val: collection)
        {
            AgeDistributionObject ageDistributionObject = val.getAgeDistributionObject();
            male_18 += ageDistributionObject.getMale_18();
            male19_29 += ageDistributionObject.getMale19_29();
            male30_39 += ageDistributionObject.getMale30_39();
            male40_ += ageDistributionObject.getMale40_();
            female_18 += ageDistributionObject.getFemale_18();
            female19_29 += ageDistributionObject.getFemale19_29();
            female30_39 += ageDistributionObject.getFemale30_39();
            female40_ += ageDistributionObject.getFemale40_();
        }

        return new AgeDistributionObject(male_18, male19_29, male30_39, male40_, female_18, female19_29, female30_39, female40_);
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

    public long getMale40_() {
        return male40_;
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

    public long getFemale40_() {
        return female40_;
    }
}
