package cs455.hadoop.util.writable;

import cs455.hadoop.util.objects.ResidenceCountObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Custom writable to store the rent count and owned count within an area
public class ResidenceCountWritable implements Writable {

    private LongWritable rentCount;
    private LongWritable ownedCount;

    public ResidenceCountWritable()
    {
        rentCount = new LongWritable(0);
        ownedCount = new LongWritable(0);
    }

    // Might be deprecated
    public ResidenceCountWritable(long rentCount, long ownedCount)
    {
        this.rentCount = new LongWritable(rentCount);
        this.ownedCount = new LongWritable(ownedCount);
    }

    public ResidenceCountWritable(ResidenceCountObject residenceCountObject)
    {
        this.rentCount = new LongWritable(residenceCountObject.getRentCount());
        this.ownedCount = new LongWritable(residenceCountObject.getOwnedCount());
    }

    // Copy Constructor
    public ResidenceCountWritable(ResidenceCountWritable residenceCountWritable)
    {
        this.rentCount = new LongWritable(residenceCountWritable.getRentCount());
        this.ownedCount = new LongWritable(residenceCountWritable.getOwnedCount());
    }

    // Getter methods
    public ResidenceCountObject getResidenceCountObject()
    {
        return new ResidenceCountObject(getRentCount(), getOwnedCount());
    }

    public long getRentCount()
    {
        return rentCount.get();
    }

    public long getOwnedCount()
    {
        return ownedCount.get();
    }

    public void setRentCount(long rentCount)
    {
        this.rentCount = new LongWritable(rentCount);
    }

    public void setOwnedCount(long ownedCount)
    {
        this.ownedCount = new LongWritable(ownedCount);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        rentCount.readFields(dataInput);
        ownedCount.readFields(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        rentCount.write(dataOutput);
        ownedCount.write(dataOutput);
    }
}