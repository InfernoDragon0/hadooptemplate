import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordExtractorReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //int result = 0;
        int result = 0;
        LongWritable res = new LongWritable();
        //for each value v in values
        for (LongWritable v : values) {
            //result += v;
            result += v.get();
            //endfor
        }
        res.set(result);
        //Emit (key, result);
        context.write(key, res);
    }
}
