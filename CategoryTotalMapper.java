import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CategoryTotalMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private final Text outKey = new Text();
    private final LongWritable outVal = new LongWritable();

    @Override
    protected void map(LongWritable offset, Text line, Context ctx)
            throws IOException, InterruptedException {
        String[] kv = line.toString().split("\t");
        String category = kv[0].split(",")[1];
        long cents = Math.round(Double.parseDouble(kv[1]) * 100);

        outKey.set(category);
        outVal.set(cents);
        ctx.write(outKey, outVal);
    }
}
