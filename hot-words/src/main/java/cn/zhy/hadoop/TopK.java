package cn.zhy.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopK {

    private static final int k = 200;

    public static class TopKMapper extends Mapper<Text, Text, Text, IntWritable> {
        //保存频率前K个的单词和单词的频率
        private List<TwoTuple<Text, IntWritable>> topk = new ArrayList<TwoTuple<Text, IntWritable>>();
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            Text kk = new Text();
            kk.set(key);
            IntWritable vv = new IntWritable(Integer.parseInt(value.toString()));

            //遍历当前的topK, 看是否要将单词插入topK当中， 下面2种情况要加入
            // topK中不足K个
            // 当前单词大于topK中频率最小的单词
            for (int i = 0; i < topk.size(); i++) {
                if (vv.compareTo(topk.get(i).count) > 0) {
                    topk.add(i, new TwoTuple(kk, vv));
                    break;
                } else if (i == topk.size() - 1 && i < k - 1) {
                    topk.add(new TwoTuple(kk, vv));
                    break;
                }  else {
                    continue;
                }
            }
            if (topk.size() == 0) {
                topk.add(new TwoTuple(kk, vv));
            } else if (topk.size() > k) {
                topk.remove(k);
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            //结束的时候把频率前K的单词输出
            for (TwoTuple<Text, IntWritable> tt : topk) {
                context.write(tt.word, tt.count);
            }
        }
    }

    public static class TwoTuple<K, V> {
        public K word;
        public V count;

        public TwoTuple(K key, V value) {
            this.word = key;
            this.count = value;
        }
    }

    public static class TopKReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private List<TwoTuple<Text, IntWritable>> topk = new ArrayList<TwoTuple<Text, IntWritable>>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            IntWritable vv = new IntWritable();
            vv.set(sum);
            Text kk = new Text();
            kk.set(key);

            for (int i = 0; i < topk.size(); i++) {
                if (vv.compareTo(topk.get(i).count) > 0) {
                    topk.add(i, new TwoTuple(kk, vv));
                    break;
                } else if (i == topk.size() - 1 && i < k - 1) {
                    topk.add(new TwoTuple(kk, vv));
                    break;
                } else {
                    continue;
                }
            }

            if (topk.size() == 0) {
                topk.add(new TwoTuple(kk, vv));
            } else if (topk.size() > k) {
                topk.remove(k);
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (TwoTuple<Text, IntWritable> tt : topk) {
                context.write(tt.word, tt.count);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: topk <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "topk");
        job.setJarByClass(TopK.class);
        job.setMapperClass(TopKMapper.class);
        job.setReducerClass(TopKReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.setInputPaths(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}