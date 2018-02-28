/**
 * Created by shwetimahajan on 2/26/18.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.io.IntWritable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class ProjectYearFilterKQuery{


    public static class CountAndName implements Writable{
        Integer count;
        String Name;
        CountAndName(Integer c, Text n) {
            count = c;
            Name = n.toString();
        }

        public String getName() {
            return Name;
        }

        public Integer getCount() {
            return count;
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(count);
            dataOutput.writeUTF(String.valueOf(Name));
        }

        public void readFields(DataInput dataInput) throws IOException {
            count = dataInput.readInt();
            Name = dataInput.readUTF();
        }
    }

    public static class CountComparator implements Comparator<CountAndName>{
        public int compare(CountAndName o1, CountAndName o2) {
            if(o1.getCount() < o2.getCount())
                return 1;
            if (o1.getCount()>o2.getCount())
                return -1;
            return 0;
        }
    }

    public static class YearFilterMapper extends Mapper<Object, Text, Text, IntWritable>{

        private String filterText;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            this.filterText = context.getConfiguration().get("filter-text"); // Year, Country, State, Gender that the query needs to filter
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String values[] = value.toString().split(",");
            String year = values[2];
            int count = 0;
            if(values.length > 4) count = Integer.parseInt(values[4]);
            if (year.equals(filterText)) {
                Text name = new Text();
                name.set(values[3]);
                IntWritable n_count = new IntWritable();
                n_count.set(count);
                context.write(name, n_count);
            }
        }

    }

    public static class KTopNameReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        int K;
        ArrayList<CountAndName> al = new ArrayList<CountAndName>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            this.K = context.getConfiguration().getInt("k", 10);
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            System.out.println(key);
            System.out.println(sum);
            al.add(new CountAndName(sum, key));
            Collections.sort(al,new CountComparator());
            System.out.println("Size of arrayList"+al.size());
        }


        public void cleanup(Context context) throws IOException, InterruptedException {
//            Collections.sort(hm, new Comparator<CountAndName>() {
//                public int compare(CountAndName o1, CountAndName o2) {
//                    return o1.getCount().compareTo(o2.getCount());
//                }
//            });
            for(int i = 0; i < K; i++) {
                IntWritable n_c = new IntWritable();
                n_c.set(al.get(i).getCount());
                Text n = new Text();
                n.set(al.get(i).getName());
                context.write(n, n_c);
            }
        }

    }

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        long start = System.currentTimeMillis();
        Job job = Job.getInstance(conf, "YearFilter");
        job.setJarByClass(ProjectYearFilterKQuery.class);
        job.setMapperClass(YearFilterMapper.class);
        //job.setNumReduceTasks(0);
        //job.setCombinerClass(KTopNameReducer.class);
        job.setReducerClass(KTopNameReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        Path input = new Path(args[0]);
        FileInputFormat.addInputPath(job, input);
        Path output = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, output);
        String status = args[2];
        int k = Integer.parseInt(args[3]);
        job.getConfiguration().set("filter-text", status);
        job.getConfiguration().setInt("k", k);
        long end = System.currentTimeMillis();
        System.out.println("Time taken : " + (end - start));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
