import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class WordExtractorMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    /**
     * DIS EXTRACTS THE WORDS FROM THE TXT FILE AND ONLY EXTRACTS WORDS THAT ARE LONGER THAN 3
     * AND SHORTER THAN 20, AND ALSO ONLY ALPHABETICAL
     * AND ALSO STOPWORD REMOVAL
     * AND ALSO DICTIONARY CHECKING
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("key is " + value.toString());
        StringTokenizer itr = new StringTokenizer(value.toString());
        LongWritable one = new LongWritable(1);
        Text word = new Text();
        List<String> document = new ArrayList<String>();
        //for each word w in value
        while(itr.hasMoreTokens()) {
            //w < w.replaceAll(“[^A-Za-z]+$”, “”).trim();
            String w = itr.nextToken().toLowerCase();
            w = w.replaceAll("[^A-Za-z]+$", "").trim();

            //if (w.length() < 4 || w.length() > 20)
            if (w.length() < 4 || w.length() > 20) {
                //w < w.replaceAll(w, “”).replaceAll(“\\s”+ “ ”).trim();
                w = w.replaceAll(w, "").replaceAll("\\s", " ").trim();
            }

            //stopword
            if (!WordCount.stopwords.contains(w)) {
                //dict check
                if (WordCount.dict.contains(w)) {
                    word.set(w);
                    if (w.length() > 0) {
                        document.add(w);
                        int whichTopic = new Random().nextInt(5);
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
                    context.write(word, one); //corpus is now complete
                }
            }
        }

        //go through each document and create topic


        //            String w = itr.nextToken();
        //            word.set(w);
        //            context.write(word, new LongWritable(whichTopic)); // 5 topics
        //
        //            theTopic.add(w);


        System.out.println(Arrays.toString(document.toArray()));
        WordCount.documents.add(document);

        System.out.println(WordCount.topics.size());
        for (Map.Entry<Integer, List<String>> entrySet : WordCount.topics.entrySet()) {
            System.out.println("Topic " + entrySet.getKey() + ": "  + Arrays.toString(entrySet.getValue().toArray()));
        }

        //TODO FOR EACH DOCUMENT gO THROUGH EACH WORD
        //AND FIND p(topic t | document d): the proportion of words in document d that are assigned to topic t
        //( #words in d with t +alpha/ #words in d with any topic+ k*alpha)
        // int update1 = (similarWordCount + topicAlpha) / nonSimilarWordCount + 5*topicAlpha

        //AND FIND p(word w| topic t): the proportion of assignments to topic t over all documents that come from this word w.
        //p(word w with topic t) = p(topic t | document d) * p(word w | topic t)
    }
}
