package cs455.hadoop.util.writable;

import cs455.hadoop.util.objects.RenterAgeDistributionObject;
import cs455.hadoop.util.objects.RoomCountObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RenterAgeDistributionWritable implements Writable{
    private LongWritable[] countArray = new LongWritable[7];

    public RenterAgeDistributionWritable()
    {
        for (int i = 0; i < 7; i++)
        {
            countArray[i] = new LongWritable(0);
        }
    }

    // If input length not 7, perform empty constructor
    public RenterAgeDistributionWritable(long[] countArray) {
        if (countArray.length != 7)
        {
            for (int i = 0; i < 7; i++)
            {
                this.countArray[i] = new LongWritable(0);
            }
        }
        else
        {
            for (int i = 0; i < 7; i++)
            {
                this.countArray[i] = new LongWritable(countArray[i]);
            }
        }
    }

    public RenterAgeDistributionWritable(RenterAgeDistributionObject renterAgeDistributionObject)
    {
        long[] countArray = renterAgeDistributionObject.getCountArray();
        for (int i = 0; i < 7; i++)
        {
            this.countArray[i] = new LongWritable(countArray[i]);
        }
    }

    // Getter classes
    public RenterAgeDistributionObject getRenterAgeDistributionObject()
    {
        return new RenterAgeDistributionObject(getCountArray());
    }

    public long[] getCountArray()
    {
        long[] countArray = new long[7];
        for (int i = 0; i < 7; i++)
        {
            countArray[i] = this.countArray[i].get();
        }
        return countArray;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        for (int i = 0; i < 7; i++)
        {
            this.countArray[i].readFields(dataInput);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        for (int i = 0; i < 7; i++)
        {
            this.countArray[i].write(dataOutput);
        }
    }
}
