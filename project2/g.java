
    @Override
    public void write(DataOutput out) throws IOException, NullPointerException {
        // TODO Auto-generated method stub

        out.writeShort(depth);
        out.writeLong(centroid);
        out.writeLong(id);
        try{
        out.writeInt(adjacent.size());
        for (long val : adjacent) {
            out.writeLong(val);
        }
        }
        catch(Exception e)
        {
            System.out.println(e);
        }
    }

        public String toString() {
        return centroid  + " " + id + " " + adjacent.toString()+" "+depth;
    }
}

public class GraphPartition {
    static Vector<Long> centroids = new Vector<Long>();
    final static short max_depth = 8;
    static short BFS_depth = 0;

    public static class First_Mapper extends Mapper<Object, Text, LongWritable, Vertex> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException, ArrayIndexOutOfBoundsException{
            String s = value.toString();
            String[] arr = s.split(",");
            Vector<Long> Adj_Array = new Vector<Long>();

            Vertex v = new Vertex();int y1=0;
            short z1=(short)y1;
            for (int i = 1; i < arr.length; i++) {
                Adj_Array.add(Long.parseLong(arr[i]));
            }

            if(arr.length>=10){
            try{
            for (int i = 0; i < 10; i++)
            {
                v = new Vertex(Long.parseLong(arr[i]), Adj_Array,Long.parseLong(arr[i]), z1);
                context.write(new LongWritable(Long.parseLong(arr[0])), v);
            }
            }
            catch(Exception e)
            {
                System.out.println(e);
            }

            int y=-1;
            long z=(long)y;

            for(int i=10;i<arr.length-1;i++)
            {
               v = new Vertex(Long.parseLong(arr[i]), Adj_Array,z, z1);
               context.write(new LongWritable(Long.parseLong(arr[0])), v);
            }
            }
            else{
                for (int i = 0; i < arr.length-1; i++) {
                v = new Vertex(Long.parseLong(arr[i]), Adj_Array,Long.parseLong(arr[i]), z1);
                context.write(new LongWritable(Long.parseLong(arr[0])), v);
            }
            }
            context.write(new LongWritable(Long.parseLong(arr[0])), v);



        }
    }

    public static class Second_Mapper extends Mapper<LongWritable, Vertex, LongWritable, Vertex> {
        public void map(LongWritable key, Vertex val, Context context) throws IOException, InterruptedException, ArrayIndexOutOfBoundsException{

            Vertex v = val;
            int y=1;
            short z=(short)y;
            context.write(new LongWritable(v.get_id()), v);
            if (v.getcentroid() > 0){
            for (long n : v.getAdj()) {
                context.write(new LongWritable(n), new Vertex(n,null,v.getcentroid(),BFS_depth));
            }}
        }
    }

    public static class First_Reducer extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {
        public void reduce(LongWritable key, Iterable<Vertex> val, Context context) throws IOException, InterruptedException, NullPointerException{
            long min_depth = 1000;
            //long x=(long)key;
            Vertex m = new Vertex(key.get());

            for (Vertex v : val)
            {
                if (v.getAdj() != null) {
                    m.setAdj(v.getAdj()) ;
                }
                if (v.getcentroid()>0 && v.getdepth()<min_depth){
                    min_depth=v.getdepth();
                    m.setcentroid(v.getcentroid());
                }

            }
            m.depth = (short)min_depth;

            context.write(new LongWritable(m.get_id()), m);
        }
    }

    public static class Final_Mapper extends Mapper<LongWritable, Vertex, LongWritable, LongWritable> {
        long a = 1;

        public void map(LongWritable key, Vertex val, Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(val.getcentroid()), new LongWritable(a));
        }
    }

    public static class Final_Reducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        public void reduce(LongWritable key, Iterable<LongWritable> val, Context context)
                throws IOException, InterruptedException {
            long m = 0;
            for (LongWritable value : val) {
                m = m + value.get();
            }
            context.write(key, new LongWritable(m));
        }
    }

    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("MyJob");
        job.setJarByClass(GraphPartition.class);

        job.setMapperClass(First_Mapper.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]+"/i0"));
        job.waitForCompletion(true);
        for ( short i = 0; i < 8; i++ ) {
            BFS_depth++;
            Job job1 = Job.getInstance();
            job1.setJobName("Second Map-Reduce Step");
            job1.setJarByClass(GraphPartition.class);

            job1.setMapperClass(Second_Mapper.class);
            job1.setReducerClass(First_Reducer.class);

            job1.setMapOutputKeyClass(LongWritable.class);
            job1.setMapOutputValueClass(Vertex.class);

            job1.setOutputKeyClass(LongWritable.class);
            job1.setOutputValueClass(Vertex.class);

            job1.setInputFormatClass(SequenceFileInputFormat.class);
            job1.setOutputFormatClass(SequenceFileOutputFormat.class);

            SequenceFileInputFormat.setInputPaths(job1, new Path(args[1]+"/i"+i));
            SequenceFileOutputFormat.setOutputPath(job1, new Path(args[1]+"/i"+(i+1)));
            job1.waitForCompletion(true);
        }
        Job job2 = Job.getInstance();
        job2.setJobName("Final Step");
        job2.setJarByClass(GraphPartition.class);

        job2.setMapperClass(Final_Mapper.class);
        job2.setReducerClass(Final_Reducer.class);

        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(LongWritable.class);

        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(LongWritable.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        SequenceFileInputFormat.setInputPaths(job2, new Path(args[1]+"/i8"));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));job.waitForCompletion(true);
        job2.waitForCompletion(true);
    }
}
