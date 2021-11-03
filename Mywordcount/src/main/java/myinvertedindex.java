import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
public class myinvertedindex {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        static enum CountersEnum { INPUT_WORDS }
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set<String> patternsToSkip = new HashSet<String>();
        private Set<String> punctuations = new HashSet<String>();
        private Configuration conf;
        private BufferedReader fis;

        @Override
        protected void setup(Context context)throws IOException, InterruptedException {//获取缓存

            Configuration conf=context.getConfiguration();
            URI [] paths = Job.getInstance(conf).getCacheFiles();
            System.out.println(paths);
            BufferedReader sw1 = new BufferedReader(new FileReader(paths[0].getPath()));
            BufferedReader sw2 = new BufferedReader(new FileReader(paths[1].getPath()));
//读取BufferedReader⾥⾯的数据,其中主要包括stop-word-list和punctuation⾥⾯的字符
            String tmp = null;
            while ((tmp = sw1.readLine()) != null) {
                String ss []= tmp.split(" ");
                for (String s : ss) {
                    li.add(s);
                } }
            while((tmp=sw2.readLine())!=null){
                String ss [] = tmp.split(" ");
                for(String s : ss){
                    li.add(s);
                } }
//关闭sw1,sw2对象
            sw1.close();
            sw2.close();
        }
        private void parseSkipFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file "
                        + StringUtils.stringifyException(ioe));
            }
        }

        private void parseSkipPunctuations(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    punctuations.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file "
                        + StringUtils.stringifyException(ioe));
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String textName = fileSplit.getPath().getName();
            String line=value.toString().toLowerCase();
            for (String pattern : punctuations) {
                line = line.replaceAll(pattern, " ");
            }
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                String this_word=itr.nextToken();
                boolean flag=false;

                if(this_word.length()<3) {
                    flag=true;
                }

                if(Pattern.compile("^[-\\+]?[\\d]*$").matcher(this_word).matches()) {
                    flag=true;
                }

                if(patternsToSkip.contains(this_word)){
                    flag=true;
                }
                if(!flag) {
                    word.set(this_word+"<"+textName);
                    context.write(word, one);
                    Counter counter = context.getCounter(
                            CountersEnum.class.getName(),
                            CountersEnum.INPUT_WORDS.toString());
                    counter.increment(1);
                }
            }
        }
    }

    public static class SortMapper //构造⽤于排序的mapper
            extends Mapper<Object, Text, tempWordCount.SortKey, IntWritable>{
        private IntWritable valueInfo = new IntWritable();
        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String tempword = value.toString();
            String[] word=tempword.split("\\s+");
            tempWordCount.SortKey keyInfo = new tempWordCount.SortKey();
            keyInfo.x=Integer.parseInt(word[word.length-1]);
            keyInfo.y=word[word.length-2];
            valueInfo.set(Integer.parseInt(word[word.length-1]));
            context.write(keyInfo, valueInfo);
        }
    }

    public static class SortReducer extends Reducer <tempWordCount.SortKey,IntWritable,Text,IntWritable> {

        private MultipleOutputs<Text,IntWritable> mos;
        private IntWritable valueInfo = new IntWritable();
        private Text keyInfo = new Text();
        private HashMap<String,Integer> map=new HashMap<>();
        protected void setup(Context context) throws IOException,InterruptedException{
            mos = new MultipleOutputs<Text,IntWritable>(context);
        }
        protected void cleanup(Context context) throws IOException,InterruptedException{
            mos.close();
        }
        @Override
        public void reduce(tempWordCount.SortKey key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            String docName=key.y.split("#")[1];
            int rank=map.getOrDefault(docName,1);
            if(rank>100){
                ;
            }
            else{
                keyInfo.set(Integer.toString(rank)+":"+key.y.split("#")[0]+", ");
                valueInfo.set(key.x);
                rank+=1;
                map.put(docName,rank);
                mos.write(docName,keyInfo,valueInfo);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    public static void main(String args[]) throws Exception{
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if ((remainingArgs.length != 2) && (remainingArgs.length != 5)) {
            System.err.println("Usage: myinvertedindex <in> <out> [-skip punctuations stopwordFile]");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(myinvertedindex.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        List<String> otherArgs = new ArrayList<String>();
        for (int i = 0; i < remainingArgs.length; ++i) {
            if ("-skip".equals(remainingArgs[i])) {
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        Path tmpdir=new Path("linshiwejian");
        FileOutputFormat.setOutputPath(job, tmpdir);
        if(job.waitForCompletion(true))
        {
            Job sortJob1 = Job.getInstance(conf, "sortword");
            sortJob1.setJarByClass(myinvertedindex.class);

            FileInputFormat.addInputPath(sortJob1, tmpdir);

            sortJob1.setMapperClass(SortMapper.class);
            sortJob1.setCombinerClass(SortReducer.class);
            sortJob1.setReducerClass(SortReducer.class);
            sortJob1.setMapOutputKeyClass(Text.class);
            sortJob1.setMapOutputValueClass(Text.class);
            FileOutputFormat.setOutputPath(sortJob1, new Path(otherArgs.get(1)));
            sortJob1.setOutputKeyClass(Text.class);
            sortJob1.setOutputValueClass(Text.class);
            System.exit(sortJob1.waitForCompletion(true) ? 0 : 1);
        }
    }
}
