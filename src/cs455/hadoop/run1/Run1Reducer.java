package cs455.hadoop.run1;

import cs455.hadoop.util.writable.Run1CombinedWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Run1Reducer extends Reducer<Text, Run1CombinedWritable, Text, Text>{

    @Override
    protected void reduce(Text key, Iterable<Run1CombinedWritable> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }
}
