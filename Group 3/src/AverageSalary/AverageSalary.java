import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class AverageSalary {
    public static class avgMapper extends Mapper < Object, Text, Text, FloatWritable > {
        private Text id = new Text();  
        private FloatWritable salary = new FloatWritable();
        public void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {
            String values[] = value.toString().split(" ");
            id.set(values[0]); // lưu trữ  các giá trị cột đầu vào  biến  id
            salary.set(Float.parseFloat(values[1]));  // lưu giá trị cột thứ 2 và biến salary
            context.write(id, salary);
        }
    }
    
    public static class avgReducer extends Reducer < Text, FloatWritable, Text, FloatWritable > {
        private FloatWritable result = new FloatWritable();
        public void reduce(Text key, Iterable < FloatWritable > values, Context context) throws IOException,
        InterruptedException {
            float sum = 0;
            float count = 0;
            for (FloatWritable val: values) {  // tính toán 
                sum += val.get();
                count++;
            }
            KQ.set(sum / count); // lưu trữ giá trị trung bình vào biến KQ
            context.write(key, KQ);
        }
    }   
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "averagesal");
        job.setJarByClass(AverageSalary.class);
        job.setMapperClass(avgMapper.class);
        job.setCombinerClass(avgReducer.class);
        job.setReducerClass(avgReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        Path p = new Path(args[0]);
        Path p1 = new Path(args[1]);
        FileInputFormat.addInputPath(job, p);
        FileOutputFormat.setOutputPath(job, p1);
        job.waitForCompletion(true);
    }
}
