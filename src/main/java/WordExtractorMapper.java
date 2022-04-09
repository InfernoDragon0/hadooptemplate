import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordExtractorMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        LongWritable one = new LongWritable(1);
        Text word = new Text();
        //for each word w in value
        while(itr.hasMoreTokens()) {
            //w < w.replaceAll(“[^A-Za-z]+$”, “”).trim();
            String w = itr.nextToken();
            w = w.replaceAll("[^A-Za-z]+$", "").trim();

            //if (w.length() < 4 || w.length() > 20)
            if (w.length() < 4 || w.length() > 20) {
                //w < w.replaceAll(w, “”).replaceAll(“\\s”+ “ ”).trim();
                w = w.replaceAll(w, "").replaceAll("\\s", " ").trim();
                //endif
            }
            word.set(w);
            //Emit (w, one);
            context.write(word, one);
            //endfor
        }
    }
}
