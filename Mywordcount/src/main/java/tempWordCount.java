import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class tempWordCount {
    public static class SortKey implements WritableComparable<SortKey> {
        public int x;
        public String y;
        public int getx() {
            return x;
        }
        public String gety() {
            return y.split("#")[0];
        }
        public void readFields(DataInput in) throws IOException {
            x = in.readInt();
            y = in.readUTF();
        }
        public void write(DataOutput out) throws IOException {
            out.writeInt(x);
            out.writeUTF(y);
        }
        public int compareTo(SortKey p) {
            if (this.x > p.x) {
                return -1;
            } else if (this.x < p.x) {
                return 1;
            } else {
                if (this.gety().compareTo(p.gety()) < 0) {
                    return -1;
                } else if (this.gety().compareTo(p.gety()) > 0) {
                    return 1;
                } else {
                    return 0;
                } }
        }
    }//建⽴⼀个⽤于排序的新类，使得mapreduce⾸先先按照value进⾏排序，对于value相同的再按照

    public static class SortRule extends IntWritable.Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        } }
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{
        private Text combineText1=new Text();
        private Text combineText2=new Text();
        private final static IntWritable one = new IntWritable(1);
        static List<String> li = new ArrayList<String>();
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
        //setup主要设置了需要略过的词的序列li
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String textName=fileSplit.getPath().getName().replaceAll("-|.txt","");
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()){
                String tmpword = itr.nextToken().replaceAll("\\d+","").toLowerCase();//去除字符串

                if((!li.contains(tmpword))&&tmpword.length()>=3)//判断单词不属于需忽略词并且⻓
                {
                    combineText1.set(tmpword+"#"+textName);
                    combineText2.set(tmpword+"#"+"total");
                    context.write(combineText1, one);
                    context.write(combineText2, one);
                } } }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
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

    public static class SortMapper //构造⽤于排序的mapper
            extends Mapper<Object, Text, SortKey, IntWritable>{
        private IntWritable valueInfo = new IntWritable();
        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String tempword = value.toString();
            String[] word=tempword.split("\\s+");
            SortKey keyInfo = new SortKey();
            keyInfo.x=Integer.parseInt(word[word.length-1]);
            keyInfo.y=word[word.length-2];
            valueInfo.set(Integer.parseInt(word[word.length-1]));
            context.write(keyInfo, valueInfo);
        }
    }
    public static class SortReducer extends Reducer <SortKey,IntWritable,Text,IntWritable> {

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
        public void reduce(SortKey key, Iterable<IntWritable> values,
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

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf,args);
        String[] otherArgs = optionParser.getRemainingArgs();
//任务⼀：⾸先多次调⽤wordcount，统计每个⽂件中的词频结果
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf);
        job.setJarByClass(tempWordCount.class);
        job.addCacheFile(new Path(otherArgs[0]+"/stopword/punctuation.txt").toUri());
        job.addCacheFile(new Path(otherArgs[0]+"/stopword/stop-word-list.txt").toUri());
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[otherArgs.length-2]+"/text"));
        Path tempFile=new Path("temp");
        FileOutputFormat.setOutputPath(job, tempFile);
        boolean flag=job.waitForCompletion(true);
//任务2 调⽤sortreducer对每个⽂件进⾏排序
        if(flag)
        {
            Configuration sortConf=new Configuration();
            Job sortJob = Job.getInstance(sortConf);
            String[] array=new File(otherArgs[0]+"/text").list();
            for(String fp : array)
            {
                MultipleOutputs.addNamedOutput(sortJob, fp.replaceAll("-|.txt",""),
                        TextOutputFormat.class,Text.class, IntWritable.class);
            }
            MultipleOutputs.addNamedOutput(sortJob,"total",TextOutputFormat.class,Text.class,IntWritable.class)
            ;
            sortJob.setJarByClass(tempWordCount.class);
            sortJob.setSortComparatorClass(SortRule.class);
            sortJob.setMapperClass(SortMapper.class);
            sortJob.setReducerClass(SortReducer.class);
            FileInputFormat.setInputPaths(sortJob, tempFile);
            FileOutputFormat.setOutputPath(sortJob, new Path(otherArgs[otherArgs.length-1]));
            sortJob.setMapOutputKeyClass(SortKey.class);
            sortJob.setMapOutputValueClass(IntWritable.class);
            sortJob.setOutputKeyClass(Text.class);
            sortJob.setOutputValueClass(IntWritable.class);
            System.exit(sortJob.waitForCompletion(true)?0:1);
        }
    }
}