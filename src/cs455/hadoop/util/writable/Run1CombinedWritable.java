package cs455.hadoop.util.writable;

import cs455.hadoop.util.objects.AgeDistributionObject;
import cs455.hadoop.util.objects.MarriageCountObject;
import cs455.hadoop.util.objects.ResidenceCountObject;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Run1CombinedWritable implements Writable{
    private ResidenceCountWritable residenceCountWritable;
    private MarriageCountWritable marriageCountWritable;
    private AgeDistributionCountWritable ageDistributionCountWritable;

    public Run1CombinedWritable()
    {
        residenceCountWritable = new ResidenceCountWritable(0,0);
        marriageCountWritable = new MarriageCountWritable(0,0,0,0);
        ageDistributionCountWritable = new AgeDistributionCountWritable(0,0,0,0,0,0,0,0);
    }

    public Run1CombinedWritable(ResidenceCountObject residenceCountObject, MarriageCountObject marriageCountObject, AgeDistributionObject ageDistributionObject)
    {
        residenceCountWritable = new ResidenceCountWritable(residenceCountObject);
        marriageCountWritable = new MarriageCountWritable(marriageCountObject);
        ageDistributionCountWritable = new AgeDistributionCountWritable(ageDistributionObject);
    }

    public AgeDistributionObject getAgeDistributionObject()
    {
        return ageDistributionCountWritable.getAgeDistributionObject();
    }

    public MarriageCountObject getMarriageCountObject()
    {
        return marriageCountWritable.getMarriageCountObject();
    }

    public ResidenceCountObject getResidenceCountObject()
    {
        return residenceCountWritable.getResidenceCountObject();
    }

    // Read in question order
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        residenceCountWritable.readFields(dataInput);
        marriageCountWritable.readFields(dataInput);
        ageDistributionCountWritable.readFields(dataInput);
    }

    // Write in question order
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        residenceCountWritable.write(dataOutput);
        marriageCountWritable.write(dataOutput);
        ageDistributionCountWritable.write(dataOutput);
    }
}
