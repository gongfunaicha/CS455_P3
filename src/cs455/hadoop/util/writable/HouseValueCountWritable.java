package cs455.hadoop.util.writable;

import cs455.hadoop.util.objects.HouseValueCountObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HouseValueCountWritable implements Writable{
    private LongWritable[] valueArray = new LongWritable[20];

    public HouseValueCountWritable()
    {
        for (int i = 0; i < 20; i++)
        {
            valueArray[i] = new LongWritable(0);
        }
    }

    // If input length not 20, perform empty constructor
    public HouseValueCountWritable(long[] valueArray) {
        if (valueArray.length != 20)
        {
            for (int i = 0; i < 20; i++)
            {
                this.valueArray[i] = new LongWritable(0);
            }
        }
        else
        {
            for (int i = 0; i < 20; i++)
            {
                this.valueArray[i] = new LongWritable(valueArray[i]);
            }
        }
    }

    public HouseValueCountWritable(HouseValueCountObject houseValueCountObject)
    {
        long[] valueArray = houseValueCountObject.getValueArray();
        for (int i = 0; i < 20; i++)
        {
            this.valueArray[i] = new LongWritable(valueArray[i]);
        }
    }

    // Getter classes
    public HouseValueCountObject getHouseValueCountObject()
    {
        return new HouseValueCountObject(getValueArray());
    }

    public long[] getValueArray()
    {
        long[] valueArray = new long[20];
        for (int i = 0; i < 20; i++)
        {
            valueArray[i] = this.valueArray[i].get();
        }
        return valueArray;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        for (int i = 0; i < 20; i++)
        {
            this.valueArray[i].readFields(dataInput);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        for (int i = 0; i < 20; i++)
        {
            this.valueArray[i].write(dataOutput);
        }
    }
}
