package cs455.hadoop.util.writable;

import cs455.hadoop.util.objects.HouseValueCountObject;
import cs455.hadoop.util.objects.RentCountObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RentCountWritable implements Writable{
    private LongWritable[] valueArray = new LongWritable[17];

    public RentCountWritable()
    {
        for (int i = 0; i < 17; i++)
        {
            valueArray[i] = new LongWritable(0);
        }
    }

    // If input length not 17, perform empty constructor
    public RentCountWritable(long[] valueArray) {
        if (valueArray.length != 17)
        {
            for (int i = 0; i < 17; i++)
            {
                this.valueArray[i] = new LongWritable(0);
            }
        }
        else
        {
            for (int i = 0; i < 17; i++)
            {
                this.valueArray[i] = new LongWritable(valueArray[i]);
            }
        }
    }

    public RentCountWritable(RentCountObject rentCountObject)
    {
        long[] valueArray = rentCountObject.getValueArray();
        for (int i = 0; i < 17; i++)
        {
            this.valueArray[i] = new LongWritable(valueArray[i]);
        }
    }

    // Getter classes
    public RentCountObject getRentCountObject()
    {
        return new RentCountObject(getValueArray());
    }

    public long[] getValueArray()
    {
        long[] valueArray = new long[17];
        for (int i = 0; i < 17; i++)
        {
            valueArray[i] = this.valueArray[i].get();
        }
        return valueArray;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        for (int i = 0; i < 17; i++)
        {
            this.valueArray[i].readFields(dataInput);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        for (int i = 0; i < 17; i++)
        {
            this.valueArray[i].write(dataOutput);
        }
    }
}
