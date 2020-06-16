import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class TableEntryMapper extends Mapper<LongWritable, Text, TableEntryBean, NullWritable> {

    Map<String, String> productid2productname = new HashMap<>();
    TableEntryBean k = new TableEntryBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //cache smaller file
        URI[] cacheFiles = context.getCacheFiles();
        String cacheFilePath = cacheFiles[0].getPath();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(cacheFilePath), "UTF-8"));
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            String[] fields = line.split("\\s+");
            productid2productname.put(fields[0], fields[1]);
        }

        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\s+");
        k.setOrder_id(fields[0]);
        k.setProduct_id(fields[1]);
        k.setProduct_name(productid2productname.get(fields[1]));
        k.setAmount(Integer.parseInt(fields[2]));

        context.write(k, NullWritable.get());
    }
}
