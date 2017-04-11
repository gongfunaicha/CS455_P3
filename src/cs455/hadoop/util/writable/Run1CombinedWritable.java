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
    private RentCountWritable rentCountWritable;
    private RoomCountWritable roomCountWritable;
    private ElderCountWritable elderCountWritable;

    public Run1CombinedWritable()
    {
        state = new Text("");
        residenceCountWritable = new ResidenceCountWritable();
        marriageCountWritable = new MarriageCountWritable();
        ageDistributionCountWritable = new AgeDistributionCountWritable();
        housePositionCountWritable = new HousePositionCountWritable();
        houseValueCountWritable = new HouseValueCountWritable();
        rentCountWritable = new RentCountWritable();
        roomCountWritable = new RoomCountWritable();
        elderCountWritable = new ElderCountWritable();
    }

    public Run1CombinedWritable(String state, ResidenceCountObject residenceCountObject, MarriageCountObject marriageCountObject, AgeDistributionObject ageDistributionObject, HousePositionCountObject housePositionCountObject, HouseValueCountObject houseValueCountObject, RentCountObject rentCountObject, RoomCountObject roomCountObject, ElderCountObject elderCountObject)
    {
        this.state = new Text(state);
        residenceCountWritable = new ResidenceCountWritable(residenceCountObject);
        marriageCountWritable = new MarriageCountWritable(marriageCountObject);
        ageDistributionCountWritable = new AgeDistributionCountWritable(ageDistributionObject);
        housePositionCountWritable = new HousePositionCountWritable(housePositionCountObject);
        houseValueCountWritable = new HouseValueCountWritable(houseValueCountObject);
        rentCountWritable = new RentCountWritable(rentCountObject);
        roomCountWritable = new RoomCountWritable(roomCountObject);
        elderCountWritable = new ElderCountWritable(elderCountObject);
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
        rentCountWritable = new RentCountWritable(run1CombinedWritable.getRentCountObject());
        roomCountWritable = new RoomCountWritable(run1CombinedWritable.getRoomCountObject());
        elderCountWritable = new ElderCountWritable(run1CombinedWritable.getElderCountObject());
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

    public RentCountObject getRentCountObject()
    {
        return rentCountWritable.getRentCountObject();
    }

    public RoomCountObject getRoomCountObject()
    {
        return roomCountWritable.getRoomCountObject();
    }

    public ElderCountObject getElderCountObject()
    {
        return elderCountWritable.getElderCountObject();
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
        rentCountWritable.readFields(dataInput);
        roomCountWritable.readFields(dataInput);
        elderCountWritable.readFields(dataInput);
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
        rentCountWritable.write(dataOutput);
        roomCountWritable.write(dataOutput);
        elderCountWritable.write(dataOutput);
    }
}
