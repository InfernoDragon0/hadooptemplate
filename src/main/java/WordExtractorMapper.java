import opennlp.tools.stemmer.PorterStemmer;
import opennlp.tools.stemmer.Stemmer;
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
     *
     * emits the entire sequence of words again for saving and reducing for wordcount
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //System.out.println("key is " + value.toString());
        StringTokenizer itr = new StringTokenizer(value.toString());
        LongWritable one = new LongWritable(1);
        Text word = new Text();
        List<String> document = new ArrayList<String>();
        //for each word w in value
        while(itr.hasMoreTokens()) {
            //w < w.replaceAll(“[^A-Za-z]+$”, “”).trim();
            String w = itr.nextToken().toLowerCase();
            w = w.replaceAll("[^A-Za-z\\s]", "").trim();

            //if (w.length() < 4 || w.length() > 20)
            if (w.length() < 4 || w.length() > 20) {
                //w < w.replaceAll(w, “”).replaceAll(“\\s”+ “ ”).trim();
                //w = w.replaceAll(w, "").replaceAll("\\s", " ").trim();
                continue;
            }

            //stopword
            if (WordCount.stopwords.contains(w)) {
                continue;
            }

            if (!WordCount.dict.contains(w)) {
                continue;
            }

            PorterStemmer stem = new PorterStemmer();
            String result = stem.stem(w);
            document.add(result);

        }

        if (document.size() > 0) {
            StringBuilder xappended = new StringBuilder();
            for (String x : document) {
                xappended.append(x).append(" ");
            }
            word.set(xappended.toString().trim());
            WordCount.docs.add(xappended.toString().trim());
        }
        context.write(word, one);
        //go through each document and create topic


        //            String w = itr.nextToken();
        //            word.set(w);
        //            context.write(word, new LongWritable(whichTopic)); // 5 topics
        //
        //            theTopic.add(w);


        //System.out.println(Arrays.toString(document.toArray()));

//        System.out.println(WordCount.topics.size());
//        for (Map.Entry<Integer, List<String>> entrySet : WordCount.topics.entrySet()) {
//            System.out.println("Topic " + entrySet.getKey() + ": "  + Arrays.toString(entrySet.getValue().toArray()));
//        }

        //TODO FOR EACH DOCUMENT gO THROUGH EACH WORD
        //AND FIND p(topic t | document d): the proportion of words in document d that are assigned to topic t
        //( #words in d with t +alpha/ #words in d with any topic+ k*alpha)
//        int similarWordCount = 0;
//        int nonSimilarWordCount = 0;
//            for (Map.Entry<Integer,List<String>> entrySet : WordCount.topics.entrySet()) {
//                for (String wordd : document) {
//
//                    if (entrySet.getValue().contains(wordd)) {
//                    similarWordCount++;
//                    }
//                    else {
//                        nonSimilarWordCount++;
//                    }
//                }
//                double update1 = (similarWordCount + WordCount.topicAlpha) / nonSimilarWordCount + (5*WordCount.topicAlpha);
//                // int update1 = (similarWordCount + topicAlpha) / nonSimilarWordCount + 5*topicAlpha
//                System.out.println("for topic " + entrySet.getKey() + ", the update is " + update1);
//            }


        //AND FIND p(word w| topic t): the proportion of assignments to topic t over all documents that come from this word w.
        //p(word w with topic t) = p(topic t | document d) * p(word w | topic t)
    }
}
