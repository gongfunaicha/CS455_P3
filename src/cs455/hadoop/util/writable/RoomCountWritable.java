package cs455.hadoop.util.writable;

import cs455.hadoop.util.objects.RoomCountObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RoomCountWritable implements Writable{
    private LongWritable[] countArray = new LongWritable[9];

    public RoomCountWritable()
    {
        for (int i = 0; i < 9; i++)
        {
            countArray[i] = new LongWritable(0);
        }
    }

    // If input length not 9, perform empty constructor
    public RoomCountWritable(long[] countArray) {
        if (countArray.length != 9)
        {
            for (int i = 0; i < 9; i++)
            {
                this.countArray[i] = new LongWritable(0);
            }
        }
        else
        {
            for (int i = 0; i < 9; i++)
            {
                this.countArray[i] = new LongWritable(countArray[i]);
            }
        }
    }

    public RoomCountWritable(RoomCountObject roomCountObject)
    {
        long[] countArray = roomCountObject.getCountArray();
        for (int i = 0; i < 9; i++)
        {
            this.countArray[i] = new LongWritable(countArray[i]);
        }
    }

    // Getter classes
    public RoomCountObject getRoomCountObject()
    {
        return new RoomCountObject(getCountArray());
    }

    public long[] getCountArray()
    {
        long[] countArray = new long[9];
        for (int i = 0; i < 9; i++)
        {
            countArray[i] = this.countArray[i].get();
        }
        return countArray;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        for (int i = 0; i < 9; i++)
        {
            this.countArray[i].readFields(dataInput);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        for (int i = 0; i < 9; i++)
        {
            this.countArray[i].write(dataOutput);
        }
    }
}
