import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PhaseOneMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(":");
        v.set(fields[0]);
        String[] followees = fields[1].split(",");
        for (String followee : followees) {
            k.set(followee);
            context.write(k, v);
        }
    }
}
