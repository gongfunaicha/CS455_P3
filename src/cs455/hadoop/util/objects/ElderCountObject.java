package cs455.hadoop.util.objects;

import cs455.hadoop.util.writable.Run1CombinedWritable;

import java.util.ArrayList;

public class ElderCountObject {
    private long elderCount;
    private long totalCount;

    public ElderCountObject() {
        elderCount = 0;
        totalCount = 0;
    }

    public ElderCountObject(long elderCount, long totalCount) {
        this.elderCount = elderCount;
        this.totalCount = totalCount;
    }

    public static ElderCountObject aggregate(ArrayList<Run1CombinedWritable> collection)
    {
        long elderCount = 0;
        long totalCount = 0;

        for (Run1CombinedWritable val: collection)
        {
            ElderCountObject elderCountObject = val.getElderCountObject();
            elderCount += elderCountObject.getElderCount();
            totalCount += elderCountObject.getTotalCount();
        }

        return new ElderCountObject(elderCount, totalCount);
    }


    public long getElderCount()
    {
        return elderCount;
    }

    public long getTotalCount()
    {
        return totalCount;
    }
}
