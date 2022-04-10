import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

public class TopicAssignerMapper extends Mapper<Text, LongWritable, Text, LongWritable> {
    /**
     * some weird algo to do topic modelling
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
//        for each word w in value
        System.out.println("key is " + key.toString());

        //randomly assign each word to a topic
        StringTokenizer itr = new StringTokenizer(key.toString());
        Text word = new Text();
        List<String> theTopic = new ArrayList<String>();
        while (itr.hasMoreTokens()) {
            int whichTopic = new Random().nextInt(5);
            String w = itr.nextToken();
            word.set(w);
            context.write(word, new LongWritable(whichTopic)); // 5 topics

            theTopic.add(w);
            //place topics in memory as well
            if (WordCount.topics.containsKey(whichTopic)) {
                if (!WordCount.topics.get(whichTopic).contains(w)) {
                    WordCount.topics.get(whichTopic).add(w);
                }
            }
            else {
                List<String> l = new ArrayList<String>();
                l.add(w);
                WordCount.topics.put(whichTopic, l);
            }
        }
    }
}
