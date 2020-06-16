import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogCleanMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (!parseLine(line, context)) {
            return;
        }
        k.set(line);
        context.write(k, NullWritable.get());
    }

    private boolean parseLine(String line, Context context) {
        String[] fields = line.split("\\s+");
        if (fields.length == 3) {
            context.getCounter("MapData", "Valid").increment(1);
            return true;
        } else {
            context.getCounter("MapData", "Invalid").increment(1);
            return false;
        }
    }
}
