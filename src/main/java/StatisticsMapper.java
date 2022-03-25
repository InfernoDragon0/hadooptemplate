import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StatisticsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text shape = new Text();
    IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value,Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        String shapeStr;

        if (parts.length == 6 && parts[3] != null) {
            shapeStr = parts[3].trim();
            if (shapeStr != null && !shapeStr.isEmpty()) {
                shape.set(shapeStr);
                context.write(shape, one);
            }
        }
    }
}
