package cs455.hadoop.run1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// Class for Map Reduce job for run 1
public class Run1Job {
    public static void main(String[] args)
    {
        try
        {
            Configuration conf = new Configuration();

            // "Residence Type" will be the job title show in yarn
            Job job = Job.getInstance(conf, "Residence Type");

            // Current class
            job.setJarByClass(Run1Job.class);
            // TODO: Mapper class
//            job.setMapperClass(ResidenceTypeMapper.class);
            // TODO: Combiner class
//            job.setCombinerClass(ResidenceTypeCombiner.class);
            // TODO: Reducer class
//            job.setReducerClass(ResidenceTypeReducer.class);
            // TODO: Set map output key class
//            job.setMapOutputKeyClass(Text.class);
            // TODO: Set map output value class
//            job.setMapOutputValueClass(ResidenceCountWritable.class);
            // TODO: Set output key class
//            job.setOutputKeyClass(Text.class);
            // TODO: Set output value class
//            job.setOutputValueClass(Text.class);
            // Add input path
            FileInputFormat.addInputPath(job, new Path(args[0]));
            // Set output path
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            // Block until job is completed
            System.exit(job.waitForCompletion(true)?0:1);
        }
        catch (IOException ioe)
        {
            System.err.println(ioe.getMessage());
        }
        catch (InterruptedException ie)
        {
            System.err.println(ie.getMessage());
        }
        catch (ClassNotFoundException cnfe)
        {
            System.err.println(cnfe.getMessage());

        }
    }
}
