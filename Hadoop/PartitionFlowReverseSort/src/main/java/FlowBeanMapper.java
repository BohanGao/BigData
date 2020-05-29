import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowBeanMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    FlowBean k = new FlowBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\s+");

        k.set(Long.parseLong(fields[fields.length-3]), Long.parseLong(fields[fields.length-2]));
        v.set(fields[1]);
        context.write(k, v);
    }
}
