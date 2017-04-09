package cs455.hadoop.util.writable;

import cs455.hadoop.util.objects.MarriageCountObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Custom writable to store the male never married count, male total count, female never married count, female total count within an area
public class MarriageCountWritable implements Writable {

    private LongWritable maleNeverMarried;
    private LongWritable maleTotal;
    private LongWritable femaleNeverMarried;
    private LongWritable femaleTotal;

    public MarriageCountWritable()
    {
        maleNeverMarried = new LongWritable(0);
        maleTotal = new LongWritable(0);
        femaleNeverMarried = new LongWritable(0);
        femaleTotal = new LongWritable(0);
    }

    // Might be deprecated
    public MarriageCountWritable(long maleNeverMarried, long maleTotal, long femaleNeverMarried, long femaleTotal)
    {
        this.maleNeverMarried = new LongWritable(maleNeverMarried);
        this.maleTotal = new LongWritable(maleTotal);
        this.femaleNeverMarried = new LongWritable(femaleNeverMarried);
        this.femaleTotal = new LongWritable(femaleTotal);
    }

    public MarriageCountWritable(MarriageCountObject marriageCountObject)
    {
        this.maleNeverMarried = new LongWritable(marriageCountObject.getMaleNeverMarried());
        this.maleTotal = new LongWritable(marriageCountObject.getMaleTotal());
        this.femaleNeverMarried = new LongWritable(marriageCountObject.getFemaleNeverMarried());
        this.femaleTotal = new LongWritable(marriageCountObject.getFemaleTotal());
    }

    // Getter methods
    public MarriageCountObject getMarriageCountObject()
    {
        return new MarriageCountObject(getMaleNeverMarriedCount(), getMaleTotalCount(), getFemaleNeverMarriedCount(), getFemaleTotalCount());
    }

    public long getMaleNeverMarriedCount()
    {
        return maleNeverMarried.get();
    }

    public long getMaleTotalCount()
    {
        return maleTotal.get();
    }

    public long getFemaleNeverMarriedCount()
    {
        return femaleNeverMarried.get();
    }

    public long getFemaleTotalCount()
    {
        return femaleTotal.get();
    }


    @Override
    public void readFields(DataInput dataInput) throws IOException {
        maleNeverMarried.readFields(dataInput);
        maleTotal.readFields(dataInput);
        femaleNeverMarried.readFields(dataInput);
        femaleTotal.readFields(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        maleNeverMarried.write(dataOutput);
        maleTotal.write(dataOutput);
        femaleNeverMarried.write(dataOutput);
        femaleTotal.write(dataOutput);
    }
}