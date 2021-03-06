package cs455.hadoop.util.writable;

import cs455.hadoop.util.objects.AgeDistributionObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Custom writable to store the male and female count based on age within an area
public class AgeDistributionCountWritable implements Writable {

    private LongWritable male_18;
    private LongWritable male19_29;
    private LongWritable male30_39;
    private LongWritable male40_84;
    private LongWritable male85_;
    private LongWritable female_18;
    private LongWritable female19_29;
    private LongWritable female30_39;
    private LongWritable female40_84;
    private LongWritable female85_;

    public AgeDistributionCountWritable()
    {
        male_18 = new LongWritable(0);
        male19_29 = new LongWritable(0);
        male30_39 = new LongWritable(0);
        male40_84 = new LongWritable(0);
        male85_ = new LongWritable(0);
        female_18 = new LongWritable(0);
        female19_29 = new LongWritable(0);
        female30_39 = new LongWritable(0);
        female40_84 = new LongWritable(0);
        female85_ = new LongWritable(0);
    }

    // Might be deprecated
    public AgeDistributionCountWritable(long male_18, long male19_29, long male30_39, long male40_84, long male85_, long female_18, long female19_29, long female30_39, long female40_84, long female85_)
    {
        this.male_18 = new LongWritable(male_18);
        this.male19_29 = new LongWritable(male19_29);
        this.male30_39 = new LongWritable(male30_39);
        this.male40_84 = new LongWritable(male40_84);
        this.male85_ = new LongWritable(male85_);
        this.female_18 = new LongWritable(female_18);
        this.female19_29 = new LongWritable(female19_29);
        this.female30_39 = new LongWritable(female30_39);
        this.female40_84 = new LongWritable(female40_84);
        this.female85_ = new LongWritable(female85_);
    }

    public AgeDistributionCountWritable(AgeDistributionObject ageDistributionObject)
    {
        this.male_18 = new LongWritable(ageDistributionObject.getMale_18());
        this.male19_29 = new LongWritable(ageDistributionObject.getMale19_29());
        this.male30_39 = new LongWritable(ageDistributionObject.getMale30_39());
        this.male40_84 = new LongWritable(ageDistributionObject.getFemale40_84());
        this.male85_ = new LongWritable(ageDistributionObject.getMale85_());
        this.female_18 = new LongWritable(ageDistributionObject.getFemale_18());
        this.female19_29 = new LongWritable(ageDistributionObject.getFemale19_29());
        this.female30_39 = new LongWritable(ageDistributionObject.getFemale30_39());
        this.female40_84 = new LongWritable(ageDistributionObject.getFemale40_84());
        this.female85_ = new LongWritable(ageDistributionObject.getFemale85_());
    }

    // Copy Constructor
//    public AgeDistributionCountWritable(AgeDistributionCountWritable ageDistributionCountWritable)
//    {
//        this.male_18 = new LongWritable(ageDistributionCountWritable.getMale_18());
//        this.male19_29 = new LongWritable(ageDistributionCountWritable.getMale19_29());
//        this.male30_39 = new LongWritable(ageDistributionCountWritable.getMale30_39());
//        this.male40_ = new LongWritable(ageDistributionCountWritable.getMale40_());
//        this.female_18 = new LongWritable(ageDistributionCountWritable.getFemale_18());
//        this.female19_29 = new LongWritable(ageDistributionCountWritable.getFemale19_29());
//        this.female30_39 = new LongWritable(ageDistributionCountWritable.getFemale30_39());
//        this.female40_ = new LongWritable(ageDistributionCountWritable.getFemale40_());
//    }

    // Getter methods
    public AgeDistributionObject getAgeDistributionObject()
    {
        return new AgeDistributionObject(getMale_18(), getMale19_29(), getMale30_39(), getMale40_84(), getMale85_(), getFemale_18(), getFemale19_29(), getFemale30_39(), getFemale40_84(), getFemale85_());
    }


    public long getMale_18()
    {
        return male_18.get();
    }

    public long getMale19_29()
    {
        return male19_29.get();
    }

    public long getMale30_39()
    {
        return male30_39.get();
    }

    public long getMale40_84()
    {
        return male40_84.get();
    }

    public long getMale85_() {
        return male85_.get();
    }

    public long getFemale_18()
    {
        return female_18.get();
    }

    public long getFemale19_29()
    {
        return female19_29.get();
    }

    public long getFemale30_39()
    {
        return female30_39.get();
    }

    public long getFemale40_84() {
        return female40_84.get();
    }

    public long getFemale85_() {
        return female85_.get();
    }

    // ReadFields override method
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        male_18.readFields(dataInput);
        male19_29.readFields(dataInput);
        male30_39.readFields(dataInput);
        male40_84.readFields(dataInput);
        male85_.readFields(dataInput);
        female_18.readFields(dataInput);
        female19_29.readFields(dataInput);
        female30_39.readFields(dataInput);
        female40_84.readFields(dataInput);
        female85_.readFields(dataInput);
    }

    // Write override method
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        male_18.write(dataOutput);
        male19_29.write(dataOutput);
        male30_39.write(dataOutput);
        male40_84.write(dataOutput);
        male85_.write(dataOutput);
        female_18.write(dataOutput);
        female19_29.write(dataOutput);
        female30_39.write(dataOutput);
        female40_84.write(dataOutput);
        female85_.write(dataOutput);
    }
}
