import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PhaseTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        v.set(fields[0]);
        String[] followers = fields[1].split(",");
        for (int i = 0; i < followers.length - 1; i++) {
            for (int j = i + 1; j < followers.length; j++) {
                k.set(followers[i].compareTo(followers[j]) < 0 ? followers[i] + "-" + followers[j] : followers[j] + "-" + followers[i]);
                context.write(k, v);
            }
        }
    }
}
