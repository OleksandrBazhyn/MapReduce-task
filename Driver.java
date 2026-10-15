import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Driver(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Path input = new Path(args[0]);
        Path out1 = new Path(args[1]);
        Path out2 = new Path(args[2]);
        int reducers1 = args.length > 3 ? Integer.parseInt(args[3]) : 1;
        boolean combiner = !(args.length > 4 && args[4].equals("off"));

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out1)) fs.delete(out1, true);
        if (fs.exists(out2)) fs.delete(out2, true);

        // Job1
        Job job1 = Job.getInstance(conf, "Job1: daily total per category");
        job1.setJarByClass(Driver.class);
        job1.setMapperClass(DailyCategoryMapper.class);
        if (combiner) job1.setCombinerClass(SumCombiner.class);
        job1.setReducerClass(DailyCategoryReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setNumReduceTasks(reducers1);
        FileInputFormat.addInputPath(job1, input);
        FileOutputFormat.setOutputPath(job1, out1);

        if (!job1.waitForCompletion(true)) return 1;

        // Job2
        Job job2 = Job.getInstance(conf, "Job2: leading category");
        job2.setJarByClass(Driver.class);
        job2.setMapperClass(CategoryTotalMapper.class);
        if (combiner) job2.setCombinerClass(SumCombiner.class);
        job2.setReducerClass(LeaderReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job2, out1);
        FileOutputFormat.setOutputPath(job2, out2);

        return job2.waitForCompletion(true) ? 0 : 1;
    }
}