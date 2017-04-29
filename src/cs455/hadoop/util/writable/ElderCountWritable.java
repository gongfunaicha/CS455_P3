package cs455.hadoop.util.writable;

import cs455.hadoop.util.objects.ElderCountObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ElderCountWritable implements Writable{
    private LongWritable elderCount;
    private LongWritable totalCount;

    public ElderCountWritable() {
        elderCount = new LongWritable(0);
        totalCount = new LongWritable(0);
    }

    public ElderCountWritable(long elderCount, long totalCount)
    {
        this.elderCount = new LongWritable(elderCount);
        this.totalCount = new LongWritable(totalCount);
    }

    public ElderCountWritable(ElderCountObject elderCountObject)
    {
        this.elderCount = new LongWritable(elderCountObject.getElderCount());
        this.totalCount = new LongWritable(elderCountObject.getTotalCount());
    }

    // Getter method
    public ElderCountObject getElderCountObject()
    {
        return new ElderCountObject(getElderCount(), getTotalCount());
    }

    public long getElderCount() {
        return elderCount.get();
    }

    public long getTotalCount() {
        return totalCount.get();
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        elderCount.readFields(dataInput);
        totalCount.readFields(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        elderCount.write(dataOutput);
        totalCount.write(dataOutput);
    }
}
