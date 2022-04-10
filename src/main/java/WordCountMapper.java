import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<Text,LongWritable, Text, LongWritable> {
    @Override
    protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(key.toString());
        LongWritable one = new LongWritable(1);
        Text word = new Text();

        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}
