package cs455.hadoop.util.writable;

import cs455.hadoop.util.objects.HousePositionCountObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HousePositionCountWritable implements Writable {
    private LongWritable urbanCount;
    private LongWritable ruralCount;
    private LongWritable otherCount;

    public HousePositionCountWritable() {
        urbanCount = new LongWritable(0);
        ruralCount = new LongWritable(0);
        otherCount = new LongWritable(0);
    }

    public HousePositionCountWritable(long urbanCount, long ruralCount, long otherCount) {
        this.urbanCount = new LongWritable(urbanCount);
        this.ruralCount = new LongWritable(ruralCount);
        this.otherCount = new LongWritable(otherCount);
    }

    public HousePositionCountWritable(HousePositionCountObject housePositionCountObject)
    {
        urbanCount = new LongWritable(housePositionCountObject.getUrbanCount());
        ruralCount = new LongWritable(housePositionCountObject.getRuralCount());
        otherCount = new LongWritable(housePositionCountObject.getOtherCount());
    }

    // Copy Constructor
//    public HousePositionCountWritable(HousePositionCountWritable housePositionCountWritable)
//    {
//        urbanCount = new LongWritable(housePositionCountWritable.getUrbanCount());
//        ruralCount = new LongWritable(housePositionCountWritable.getRuralCount());
//        otherCount = new LongWritable(housePositionCountWritable.getOtherCount());
//    }

    // Getter methods
    public HousePositionCountObject getHousePositionCountObject()
    {
        return new HousePositionCountObject(getUrbanCount(), getRuralCount(), getOtherCount());
    }

    public long getUrbanCount()
    {
        return urbanCount.get();
    }

    public long getRuralCount()
    {
        return ruralCount.get();
    }

    public long getOtherCount()
    {
        return otherCount.get();
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        urbanCount.readFields(dataInput);
        ruralCount.readFields(dataInput);
        otherCount.readFields(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        urbanCount.write(dataOutput);
        ruralCount.write(dataOutput);
        otherCount.write(dataOutput);
    }
}
