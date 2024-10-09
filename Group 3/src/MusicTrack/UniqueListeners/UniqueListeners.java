import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UniqueListeners {
  // Define the counters
  public static enum COUNTERS {
    INVALID_RECORD_COUNT
  }
  // Define mapper
  public static class UniqueListenersMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    IntWritable trackId = new IntWritable();
    IntWritable userId = new IntWritable();
    // Define the class contains id corresponding to columns.
    public class LastFMConstants {
      public static final int USER_ID = 0; // the first element in array
      public static final int TRACK_ID = 1; // the second element in array
      public static final int IS_SHARED = 2;
      public static final int RADIO = 3;
      public static final int IS_SKIPPED = 4;
    }
    // Override the mapper
    public void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
      // Split the input with '|' char
      String[] parts = value.toString().split("[|]");
      // Get the trackId and userId from record array parts
      trackId.set(Integer.parseInt(parts[LastFMConstants.TRACK_ID]));
      userId.set(Integer.parseInt(parts[LastFMConstants.USER_ID]));
      // Check if the input record array has 5 cells (elements)
      if (parts.length == 5) {
        context.write(trackId, userId); //write the trackId and userId to the context
      } else {
        //add counter for invalid records
        context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
      }
    }
  }

  public static class UniqueListenersReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    // Override the reduce
    public void reduce(IntWritable trackId, Iterable<IntWritable> userIds, 
    Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
      // Create a set to store the unique user ID
      Set<Integer> userIdSet = new HashSet<Integer>();
      // Traverse userIds and add users' id to the set
      for (IntWritable userId : userIds) {
        userIdSet.add(userId.get());
      }
      // Write the trackId and the number of the userId (set's size) to the context
      IntWritable size = new IntWritable(userIdSet.size());
      context.write(trackId, size);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    if (args.length != 2) {
      System.err.println("Usage: uniquelisteners <in> <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "Unique listeners per track");
    job.setJarByClass(UniqueListeners.class);
    job.setMapperClass(UniqueListenersMapper.class);
    job.setReducerClass(UniqueListenersReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

    org.apache.hadoop.mapreduce.Counters counters = job.getCounters();
    System.out.println("No. of Invalid Records :" + counters.findCounter(COUNTERS.INVALID_RECORD_COUNT).getValue());
  }
}