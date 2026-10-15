import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Driver <input path> <output path>");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "Job: daily total per category");
        job1.setJarByClass(Driver.class);
        job1.setMapperClass(DailyCategoryMapper.class);
        job1.setCombinerClass(SumCombiner.class);
        job1.setReducerClass(DailyCategoryReducer.class);
        
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
}