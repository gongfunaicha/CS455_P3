package cs455.hadoop.util.writable;

import cs455.hadoop.util.objects.AgeDistributionObject;
import cs455.hadoop.util.objects.MarriageCountObject;
import cs455.hadoop.util.objects.ResidenceCountObject;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Run1CombinedWritable implements Writable{
    private Text state;
    private ResidenceCountWritable residenceCountWritable;
    private MarriageCountWritable marriageCountWritable;
    private AgeDistributionCountWritable ageDistributionCountWritable;

    public Run1CombinedWritable()
    {
        state = new Text("");
        residenceCountWritable = new ResidenceCountWritable(0,0);
        marriageCountWritable = new MarriageCountWritable(0,0,0,0);
        ageDistributionCountWritable = new AgeDistributionCountWritable(0,0,0,0,0,0,0,0);
    }

    public Run1CombinedWritable(String state, ResidenceCountObject residenceCountObject, MarriageCountObject marriageCountObject, AgeDistributionObject ageDistributionObject)
    {
        this.state = new Text(state);
        residenceCountWritable = new ResidenceCountWritable(residenceCountObject);
        marriageCountWritable = new MarriageCountWritable(marriageCountObject);
        ageDistributionCountWritable = new AgeDistributionCountWritable(ageDistributionObject);
    }

    // Copy constructor
    public Run1CombinedWritable(Run1CombinedWritable run1CombinedWritable)
    {
        this.state = new Text(run1CombinedWritable.getState());
        residenceCountWritable = new ResidenceCountWritable(run1CombinedWritable.getResidenceCountObject());
        marriageCountWritable = new MarriageCountWritable(run1CombinedWritable.getMarriageCountObject());
        ageDistributionCountWritable = new AgeDistributionCountWritable(run1CombinedWritable.getAgeDistributionObject());
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

    public String getState()
    {
        return state.toString();
    }

    // Read in question order, state first
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        state.readFields(dataInput);
        residenceCountWritable.readFields(dataInput);
        marriageCountWritable.readFields(dataInput);
        ageDistributionCountWritable.readFields(dataInput);
    }

    // Write in question order, state first
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        state.write(dataOutput);
        residenceCountWritable.write(dataOutput);
        marriageCountWritable.write(dataOutput);
        ageDistributionCountWritable.write(dataOutput);
    }
}
