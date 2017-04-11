package cs455.hadoop.run1;

import cs455.hadoop.util.objects.*;
import cs455.hadoop.util.writable.Run1CombinedWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

public class Run1Combiner extends Reducer<Text, Run1CombinedWritable, Text, Run1CombinedWritable>{
    @Override
    protected void reduce(Text key, Iterable<Run1CombinedWritable> values, Context context) throws IOException, InterruptedException {
        // Group Run1CombinedWritable by State
        // Used TreeMap due to it is sorted by key
        TreeMap<String, ArrayList<Run1CombinedWritable>> groupByState = new TreeMap<>();

        for (Run1CombinedWritable val: values)
        {
            String state = val.getState();
            if (!groupByState.containsKey(state))
            {
                // If haven't seen this key, add key and empty arraylist
                groupByState.put(state, new ArrayList<>());
            }

            // Deep copy val into corresponding ArrayList
            groupByState.get(state).add(new Run1CombinedWritable(val));
        }

        // Iterator through the groupByState, emit one Run1CombinedWritable for each state
        for (Map.Entry<String, ArrayList<Run1CombinedWritable>> entry : groupByState.entrySet())
        {
            String state = entry.getKey();
            ArrayList<Run1CombinedWritable> collection = entry.getValue();

            // Generate one Run1CombinedWritable for this collection
            ResidenceCountObject aggregatedResidenceCountObject = ResidenceCountObject.aggregate(collection);
            MarriageCountObject aggregatedMarriageCountObject = MarriageCountObject.aggregate(collection);
            AgeDistributionObject aggregatedAgeDistributionObject = AgeDistributionObject.aggregate(collection);
            HousePositionCountObject aggregatedHousePositionCountObject = HousePositionCountObject.aggregate(collection);
            HouseValueCountObject aggregatedHouseValueCountObject = HouseValueCountObject.aggregate(collection);
            RoomCountObject aggregatedRoomCountObject = RoomCountObject.aggregate(collection);

            // Emit aggregated Run1CombinedWritable
            context.write(new Text("1"), new Run1CombinedWritable(state, aggregatedResidenceCountObject, aggregatedMarriageCountObject, aggregatedAgeDistributionObject, aggregatedHousePositionCountObject, aggregatedHouseValueCountObject, aggregatedRoomCountObject));
        }
    }
}
