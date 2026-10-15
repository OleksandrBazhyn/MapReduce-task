import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DailyCategoryMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private final Text outKey = new Text();
    private final LongWritable outValue = new LongWritable();

    @Override
    protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
        String[] s = line.toString()
        if (s.length != 4) {
            throw new IOException("Invalid number of columns: " + line);
        }
        if (s.startsWith("user_id")) {
            return;
        }
        
        String f = s.split(",");
        String date = f[1].substring(0, 10);
        long cents = Math.round(Double.parseDouble(f[3]) * 100);

        outKey.set(date + "," + f[2]);
        outValue.set(cents);
        context.write(outKey, outValue);
    }
}