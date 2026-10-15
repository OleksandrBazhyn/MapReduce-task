import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LeaderReducer extends Reducer<Text, LongWritable, Text, Text> {
    private final Map<String, Long> totals = new HashMap<>();

    @Override
    protected void reduce(Text category, Iterable<LongWritable> vals, Context ctx) {
        long sum = 0;
        for (LongWritable v : vals) sum += v.get();
        totals.put(category.toString(), sum);
    }

    @Override
    protected void cleanup(Context ctx) throws IOException, InterruptedException {
        List<Map.Entry<String, Long>> list = new ArrayList<>(totals.entrySet());
        list.sort((a, b) -> Long.compare(b.getValue(), a.getValue()));
        int rank = 1;
        for (Map.Entry<String, Long> e : list) {
            long s = e.getValue();
            ctx.write(new Text(String.valueOf(rank++)),
                      new Text(e.getKey() + "\t" + String.format("%d.%02d", s / 100, s % 100)));
        }
    }
}
