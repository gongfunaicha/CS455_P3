package cs455.hadoop.util.writable;

import cs455.hadoop.util.objects.*;
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
    private HousePositionCountWritable housePositionCountWritable;
    private HouseValueCountWritable houseValueCountWritable;

    public Run1CombinedWritable()
    {
        state = new Text("");
        residenceCountWritable = new ResidenceCountWritable();
        marriageCountWritable = new MarriageCountWritable();
        ageDistributionCountWritable = new AgeDistributionCountWritable();
        housePositionCountWritable = new HousePositionCountWritable();
        houseValueCountWritable = new HouseValueCountWritable();
    }

    public Run1CombinedWritable(String state, ResidenceCountObject residenceCountObject, MarriageCountObject marriageCountObject, AgeDistributionObject ageDistributionObject, HousePositionCountObject housePositionCountObject, HouseValueCountObject houseValueCountObject)
    {
        this.state = new Text(state);
        residenceCountWritable = new ResidenceCountWritable(residenceCountObject);
        marriageCountWritable = new MarriageCountWritable(marriageCountObject);
        ageDistributionCountWritable = new AgeDistributionCountWritable(ageDistributionObject);
        housePositionCountWritable = new HousePositionCountWritable(housePositionCountObject);
        houseValueCountWritable = new HouseValueCountWritable(houseValueCountObject);
    }

    // Copy constructor
    public Run1CombinedWritable(Run1CombinedWritable run1CombinedWritable)
    {
        this.state = new Text(run1CombinedWritable.getState());
        residenceCountWritable = new ResidenceCountWritable(run1CombinedWritable.getResidenceCountObject());
        marriageCountWritable = new MarriageCountWritable(run1CombinedWritable.getMarriageCountObject());
        ageDistributionCountWritable = new AgeDistributionCountWritable(run1CombinedWritable.getAgeDistributionObject());
        housePositionCountWritable = new HousePositionCountWritable(run1CombinedWritable.getHousePositionCountObject());
        houseValueCountWritable = new HouseValueCountWritable(run1CombinedWritable.getHouseValueCountObject());
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

    public HousePositionCountObject getHousePositionCountObject()
    {
        return housePositionCountWritable.getHousePositionCountObject();
    }

    public HouseValueCountObject getHouseValueCountObject()
    {
        return houseValueCountWritable.getHouseValueCountObject();
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
        housePositionCountWritable.readFields(dataInput);
        houseValueCountWritable.readFields(dataInput);
    }

    // Write in question order, state first
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        state.write(dataOutput);
        residenceCountWritable.write(dataOutput);
        marriageCountWritable.write(dataOutput);
        ageDistributionCountWritable.write(dataOutput);
        housePositionCountWritable.write(dataOutput);
        houseValueCountWritable.write(dataOutput);
    }
}
