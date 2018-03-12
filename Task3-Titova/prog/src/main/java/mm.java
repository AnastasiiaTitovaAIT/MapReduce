import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.URI;
import java.util.HashMap;

public class mm extends Configured implements Tool {
    public static class mmFirstMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        private String tags;
        private int totalI;
        private int totalJ;
        private int totalK;
        private int groups;
        int IperGroup;
        int JperGroup;
        int KperGroup;
        int resI;
        int resJ;
        int resK;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] content = value.toString().split("\t");
            float elem = Float.parseFloat(content[3]);
            StringBuilder outkey = new StringBuilder();
            int count;
            int i;
            boolean flag1, flag2;
            String outval;
            if (tags.startsWith(content[0])){
                i = 0; count = 0;
                flag1 = true;
                flag2 = true;
                int iInd = Integer.parseInt(content[1]);
                int jInd = Integer.parseInt(content[2]);
                int iGroupId=-1;
                int jGroupId=-1;
                while(count<2){
                    if(i==groups-1){
                        if(flag1){ iGroupId = i; count++; flag1=false;}
                        if(flag2){ jGroupId = i; count++; flag2 = false;}
                    }
                    if(iInd >= i*IperGroup && iInd <= (i+1)*IperGroup - 1){
                        iGroupId=i;
                        count++;
                        flag1 = false;
                    }
                    if(jInd >= i*JperGroup && jInd <= (i+1)*JperGroup - 1){
                        jGroupId = i;
                        count++;
                        flag2 = false;
                    }
                    i++;
                }
		outkey.setLength(0);
                for(int m=0; m<groups;m++){
                    outkey.append(iGroupId);
                    outkey.append("\t");
                    outkey.append(jGroupId);
                    outkey.append("\t");
                    outkey.append(m);
                    outval = content[0] + "\t" + iInd + "\t" + jInd + "\t" + elem;
                    context.write(new Text(outkey.toString()), new Text(outval));
                    outkey.setLength(0);
                }
            }else{
                i = 0; count = 0;
                flag1 = true;
                flag2 = true;
                int jInd = Integer.parseInt(content[1]);
                int kInd = Integer.parseInt(content[2]);
                int jGroupId=-1;
                int kGroupId=-1;
                while(count<2){
                    if(i==groups-1){
                        if(flag1){ jGroupId = i; count++; flag1 = false;}
                        if(flag2){ kGroupId = i; count++; flag2 = false;}
                    }
                    if(jInd >= i*JperGroup && jInd <= (i+1)*JperGroup - 1){
                        jGroupId = i;
                        count++;
                        flag1 = false;
                    }
                    if(kInd >= i*KperGroup && kInd <= (i+1)*KperGroup - 1){
                        kGroupId = i;
                        count++;
                        flag2 = false;
                    }
                    i++;
                }
		outkey.setLength(0);
                for(int m=0; m<groups;m++){
                    outkey.append(m);
                    outkey.append("\t");
                    outkey.append(jGroupId);
                    outkey.append("\t");
                    outkey.append(kGroupId);
                    outval = content[0] + "\t"+ jInd + "\t"+ kInd + "\t"+ elem;
                    context.write(new Text(outkey.toString()), new Text(outval));
                    outkey.setLength(0);
                }
            }
        }
        @Override
        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            totalI = Integer.parseInt(conf.get("mm.arows"));
            totalJ = Integer.parseInt(conf.get("mm.acols"));
            totalK = Integer.parseInt(conf.get("mm.bcols"));
            tags = conf.get("mm.tags","ABC");
            groups = conf.getInt("mm.groups", 1);

            IperGroup = totalI/groups;
            JperGroup = totalJ/groups;
            KperGroup = totalK/groups;
            resI = totalI%groups;
            resJ = totalJ%groups;
            resK = totalK%groups;
        }
    }

    public static class mmFirstReducer
            extends Reducer<Text, Text, Text, Text> {
        int groups;
        private String tags;
        private int totalI;
        private int totalJ;
        private int totalK;
        int IperGroup;
        int JperGroup;
        int KperGroup;
        int resI;
        int resJ;
        int resK;
        private Text result=new Text();
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            HashMap<String, Float> A = new HashMap<>();
            HashMap<String, Float> B = new HashMap<>();
            String mapkey;
            String [] content;
            String [] tmp;
            tmp = key.toString().split("\t");
            int idI = Integer.parseInt(tmp[0]);
            int idJ = Integer.parseInt(tmp[1]);
            int idK = Integer.parseInt(tmp[2]);
            for (Text val : values) {
                content = val.toString().split("\t");
                float w = Float.parseFloat(content[3]);
                mapkey = content[1]+"\t"+content[2];
                if (tags.startsWith(content[0])){
                    A.put(mapkey,w);
                } else {
                    B.put(mapkey, w);
                }
            }
            int I = IperGroup;
            int J = JperGroup;
            int K = KperGroup;
            if (idI == groups-1) I += resI;
            if (idJ == groups-1) J += resJ;
            if (idK == groups-1) K += resK;
	    
            StringBuilder outkey = new StringBuilder();

            for(int i=0;i<I;i++)
                for(int k=0;k<K;k++){
                    float sum=0.0f;
                    for(int j=0;j<J;j++){
			int indi=i+IperGroup*idI;
			int indj=j+JperGroup*idJ;
			int indk=k+KperGroup*idK;
                        String keyA = indi + "\t" + indj;
                        String keyB = indj + "\t" + indk;
                        if(A.containsKey(keyA) &&
                                B.containsKey(keyB))
                           { sum+=A.get(keyA)*B.get(keyB);}
			    
                    }
		            outkey.append(i+IperGroup*idI);
                    outkey.append("\t");
                    outkey.append(k+KperGroup*idK);
                    if(sum != 0.0f) {
                        result.set(String.valueOf(sum));
                        context.write(new Text(outkey.toString()), result);
                    }
                    outkey.setLength(0);
                }
        }
        @Override
        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            groups = conf.getInt("mm.groups", 1);
            tags = conf.get("mm.tags","ABC");
            totalI = Integer.parseInt(conf.get("mm.arows"));
            totalJ = Integer.parseInt(conf.get("mm.acols"));
            totalK = Integer.parseInt(conf.get("mm.bcols"));
            IperGroup = totalI/groups;
            JperGroup = totalJ/groups;
            KperGroup = totalK/groups;
            resI = totalI%groups;
            resJ = totalJ%groups;
            resK = totalK%groups;

        }
    }
    public static class mmSecondMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
		String [] cont = value.toString().split("\t");
		String newkey  = cont[0]+"\t"+cont[1];
		String newvalue = cont[2];            
		context.write(new Text(newkey), new Text(newvalue));
        }
    }

    public static class mmSecondReducer
            extends Reducer<Text, Text, Text, Text> {
        Text result = new Text();
        String frmt;
        private char[] tags;
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            float sum = 0.0f;
            StringBuilder outkey = new StringBuilder();
            outkey.append(tags[2]);
            outkey.append("\t");
            outkey.append(key);
            for (Text val : values) {
                sum += Float.parseFloat(val.toString());
            }
            result.set(String.format(frmt, sum));
            context.write(new Text(outkey.toString()), result);
        }
        @Override
        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            frmt = conf.get("mm.float-format", "%.3f");
            tags = conf.get("mm.tags","ABC").toCharArray();
        }
    }
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(URI.create(args[0]), conf);
        Path pt = new Path(args[0] + "/size");
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(pt)));
        String content = "";
        try {
            content = reader.readLine();
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            reader.close();
        }

        String[] size = content.split("\t");
        int rows = Integer.parseInt(size[0]), cols = Integer.parseInt(size[1]);
        conf.setInt("mm.arows", rows);
        conf.setInt("mm.acols", cols);
        conf.setInt("mm.crows", rows);
        conf.setInt("mm.brows", cols);
        fs = FileSystem.get(URI.create(args[1]), conf);
        pt = new Path(args[1] + "/size");
        reader = new BufferedReader(new InputStreamReader(fs.open(pt)));
        content = "";
        try {
            content = reader.readLine();
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            reader.close();
        }
        size = content.split("\t");
        cols = Integer.parseInt(size[1]);
        conf.setInt("mm.bcols", cols);
        conf.setInt("mm.ccols", cols);

        fs = FileSystem.get(URI.create(args[2]), conf);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(args[2] + "/size"))));
        String input = conf.get("mm.crows") + "\t" + conf.get("mm.ccols") + "\n";
        try {
            writer.write(input);
            writer.close();
        } catch (IOException exc) {
            exc.printStackTrace();
        } finally {
            writer.close();
        }
        Job job = Job.getInstance(conf, "matrix multiplication");
        job.setJarByClass(mm.class);
        job.setMapperClass(mmFirstMapper.class);
        job.setReducerClass(mmFirstReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(conf.getInt("mm.num-reducers", 1));
        FileInputFormat.addInputPath(job, new Path(args[0] + "/data"));
        FileInputFormat.addInputPath(job, new Path(args[1] + "/data"));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/tmpdata"));
        if (!job.waitForCompletion(true))
        {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "matrix multiplication");
        job2.setJarByClass(mm.class);
        job2.setMapperClass(mmSecondMapper.class);
        job2.setReducerClass(mmSecondReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setNumReduceTasks(conf.getInt("mm.num-reducers", 1));
        FileInputFormat.addInputPath(job2, new Path(args[1] + "/tmpdata"));
        FileOutputFormat.setOutputPath(job2, new Path(args[2] + "/data"));
        if (!job2.waitForCompletion(true))
        {
            System.exit(1);
        }
        return 0;
    }
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new mm(), args);
        System.exit(res);
    }
}
