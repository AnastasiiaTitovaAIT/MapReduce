import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class mgen extends Configured implements Tool {
        
    public static class GenSplit extends InputSplit implements Writable {
        private IntWritable id=new IntWritable(0);
	    private LongWritable mapSeed=new LongWritable(0);
	    public void setId (IntWritable val) { id = val;}
	    public void setMapSeed (LongWritable seed) { mapSeed = seed;}
        public IntWritable getId  () {return id;}
        public LongWritable getMapSeed () {return mapSeed;}
	    @Override
        public void write(DataOutput out) throws IOException { id.write(out); mapSeed.write(out);}
        @Override
        public void readFields(DataInput in) throws IOException { id.readFields(in); mapSeed.readFields(in); }
        @Override
        public long getLength() { return 0L; }
        @Override
        public String[] getLocations() { return new String[0]; }
    }
    
    static class GenRecordReader extends RecordReader<Text, Text> {
        private int numMap;
        private int rows;
        private int rowsinmap;      
        private int lastMaptask;
        private int cnt;
		private Configuration conf;                
		private Text key = new Text(" ");
        private Text value;
		private boolean processed = false;	
		
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) {
			this.conf = context.getConfiguration();			
			this.numMap = conf.getInt("mgen.num-mappers", 1);
			this.rows  = conf.getInt("mgen.row-count", 2);
			this.rowsinmap = rows/numMap;
			this.lastMaptask = rows%numMap;
			this.cnt = (((GenSplit)split).getId()).get();
			value = new Text(String.valueOf((((GenSplit)split).getMapSeed()).get()));
		}

        @Override
        public boolean nextKeyValue()
        throws IOException {
            if(!processed){
			 	if (cnt == numMap) {
                    key.set(String.valueOf((cnt-1)*rowsinmap)+"\t"+String.valueOf((cnt-1)*rowsinmap+rowsinmap-1+lastMaptask));
                    cnt++;
                }
                else{
                    key.set(String.valueOf((cnt-1)*rowsinmap)+"\t"+String.valueOf((cnt-1)*rowsinmap+rowsinmap-1));
				    cnt++;
                }
				processed = true;                
				return true;
			}
			return false;
		}
            @Override
            public Text getCurrentKey() { return key; }
            @Override
            public Text getCurrentValue() { return value; }
            @Override
            public void close() throws IOException { }
            @Override
            public float getProgress() throws IOException, InterruptedException {
                return processed?1.0f:0.0f;
            }
    }

    public static class mgenInputFormat extends InputFormat<Text, Text> {
        @Override
        public List<InputSplit> getSplits(JobContext jobContext) {
            List<InputSplit> ret = new ArrayList<>();
            int numSplits = jobContext.getConfiguration().getInt("mgen.num-mappers", 1);
            long seed = jobContext.getConfiguration().getLong("seed", System.currentTimeMillis());
            Random rand = new Random(seed);
            for (int i = 0; i < numSplits; ++i) {
                GenSplit nsplit = new GenSplit();
                nsplit.setMapSeed(new LongWritable(rand.nextLong()));
                nsplit.setId(new IntWritable(i+1));
                ret.add(nsplit);
            }
            return ret;
        }

        @Override
        public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext taskContext) throws IOException {
            return new GenRecordReader();
        }
    }

    public static class GenMapper
            extends Mapper<Text, Text, Text, Text>{
        private int cols;
        private double min;
        private double max;
        private double sparsity;
        private String frmt;
        private String tag = null;
        private long seed;
        private Random random;

        public void map(Text inkey, Text invalue, Context context)
                throws IOException, InterruptedException {
            Text key = new Text();
            Text value = new Text();
	        seed = Long.parseLong(invalue.toString());
            random = new Random(seed);
            String [] ind= inkey.toString().split("\t");
            int startRow = Integer.parseInt(ind[0]);
            int endRow = Integer.parseInt(ind[1]);
            int toGen = (int) ((endRow-startRow+1)*cols*(1-sparsity));
            int i, j;
            IndPair cur;
            StringBuilder k = new StringBuilder();
            double rnd;
            double result;
	        ArrayList<IndPair> genInd = new ArrayList<>();
	        if(sparsity==0) {
                for(int m = startRow; m<endRow+1; m++){
                    for(int l = 0; l<cols; l++){
                        if (tag != null) {
                            k.append(tag);
                            k.append("\t");
                        }
                        k.append(m);
                        k.append("\t");
                        k.append(l);
                        rnd = random.nextDouble();
                        result = min +(rnd*(max - min));
                        key.set(k.toString());
                        value.set(String.format(frmt, result));
                        context.write(key, value);
                        k.setLength(0);
                    }
                }
            } else {
                while (genInd.size() < toGen) {
                    if (tag != null) {
                        k.append(tag);
                        k.append("\t");
                    }
                    rnd = random.nextDouble();
                    result = min +(rnd*(max - min));
                    key.set(k.toString());
                    value.set(String.format(frmt, result));
                    context.write(key, value);
                    i = ThreadLocalRandom.current().nextInt(startRow, endRow + 1);
                    j = ThreadLocalRandom.current().nextInt(0, cols);
                    cur = new IndPair(i, j);
                    if (!genInd.contains(cur)) {
                        genInd.add(cur);
                        k.append(i);
                        k.append("\t");
                        k.append(j);
                        rnd = random.nextDouble();
                        result = min + (rnd * (max - min));
                        key.set(k.toString());
                        value.set(String.format(frmt, result));
                        context.write(key, value);
                        k.setLength(0);
                    }
                }
            }
        }
	    @Override
	    public void setup(Context context){
            Configuration conf = context.getConfiguration();
            cols = conf.getInt("mgen.column-count", 2);
            min = conf.getDouble("mgen.min", 0.);
            max = conf.getDouble("mgen.max", 1.);
            sparsity = conf.getDouble("mgen.sparsity", 0.);
            frmt = conf.get("mgen.float-format", "%.3f");
            tag = conf.get("mgen.tag");
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(URI.create(args[0]), conf);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(args[0]+"/size"))));
        String input =  String.valueOf(conf.getInt("mgen.row-count", 2)) + "\t" + String.valueOf(conf.getInt("mgen.column-count", 2))+"\n";
        try {
            writer.write(input);
            writer.close();
        } catch (IOException exc) {
            exc.printStackTrace();
        } finally {
            writer.close();
        }
        Job job = new Job(conf, "mgen");
        job.setJarByClass(mgen.class);
        job.setMapperClass(GenMapper.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(mgenInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[0]+"/data"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
    public static void main(String[] args) throws Exception {      
	    int res = ToolRunner.run(new Configuration(), new mgen(), args);
        System.exit(res);
    }
}
