import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableEntryMapper extends Mapper<LongWritable, Text, Text, TableEntryBean> {

    String fileName;
    Text k = new Text();
    TableEntryBean v = new TableEntryBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        fileName = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\s+");
        if (fileName.startsWith("order")) {
            v.setOrder_id(fields[0]);
            v.setProduct_id(fields[1]);
            v.setProduct_name("");
            v.setAmount(Integer.parseInt(fields[2]));
            v.setFlag("Order");
        } else {//product
            v.setOrder_id("");
            v.setProduct_id(fields[0]);
            v.setProduct_name(fields[1]);
            v.setAmount(0);
            v.setFlag("Product");
        }
        k.set(v.getProduct_id());
        context.write(k, v);
    }
}
