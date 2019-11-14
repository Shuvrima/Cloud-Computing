import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
//This code is written with help of pseudocode provided on class website
//author: Shuvrima Alam

 class Vertex implements Writable
{
    public long id;                   // the vertex ID
    public Vector<Long> adjacent;     // the vertex neighbors
    public long centroid;             // the id of the centroid in which this vertex belongs to
    public short depth;               // the BFS depth

    public Vertex() {} //constructors 

    public Vertex(long id, Vector<Long> adj,long centroid,short depth)
    {

    	this.id=id;
    	this.adjacent=adj;
    	this.centroid=centroid;
    	this.depth=depth;
    }

    public void readFields ( DataInput in ) throws IOException
    {
    	adjacent= new Vector<Long>();

        this.id = in.readLong();
        int size =in.readInt();

        for(int i=0; i<size; i++)
        {
        	this.adjacent.addElement(in.readLong());
        }

        this.centroid = in.readLong();
        this.depth = in.readShort();
    }

    public void write ( DataOutput out ) throws IOException
    {

        out.writeLong(this.id);
        out.writeInt(this.adjacent.size());

        for(int i=0; i<this.adjacent.size(); i++)
        {
        	out.writeLong(this.adjacent.get(i));
        }

        out.writeLong(this.centroid);
        out.writeShort(this.depth);

    }

}

public class GraphPartition
{
    static Vector<Long> centroids = new Vector<Long>();
    final static short max_depth = 8;
    static short BFS_depth = 0;

    

    public static class FirstMapper extends Mapper<Object,Text,LongWritable,Vertex>
    {
    	int i=1;
    	@Override
    	public void map(Object key, Text value, Context context)throws IOException, InterruptedException
    	{


    		short depth=0;
    		long centroid;
    		Vector<Long> adjacent= new Vector<Long>();
    		Scanner scanner=new Scanner(value.toString()).useDelimiter(",");
    		long id=scanner.nextLong();

    		if(i<=10)
    		{
    			centroid = id;
    			i++;
    		}
    		else
    		{
    			centroid= -1;
    		}

    		while(scanner.hasNext())
    		{
    			adjacent.addElement(scanner.nextLong());
    		}

    		context.write(new LongWritable(id), new Vertex(id,adjacent,centroid,depth));
    	}
    }

    

    public static class SecondMapper extends Mapper<LongWritable,Vertex,LongWritable,Vertex>
    {
    	@Override
    	public void map(LongWritable key, Vertex vertex, Context context)throws IOException, InterruptedException
    	{
    		context.write(new LongWritable(vertex.id), vertex);

    		if (vertex.centroid > 0)
    		{
    			for(Long val: vertex.adjacent)
    			{
    				Vector<Long> v_adj= new Vector<Long>();
    				context.write(new LongWritable(val), new Vertex(val,v_adj,vertex.centroid,BFS_depth));
    			}
    		}

    	}

    }

    public static class SecondReducer extends Reducer<LongWritable,Vertex,LongWritable,Vertex>
    {
    	@Override
    	public void reduce(LongWritable id, Iterable<Vertex> values, Context context)throws IOException, InterruptedException
    	{
    		short min_depth = 1000;
    		long centroid=-1;
    		short depth=0;
    		long id1=id.get();
    		Vector<Long> v_set= new Vector<Long>();

    		Vertex m = new Vertex(id1,v_set,centroid,depth);

    		for(Vertex v: values)
    		{
    			if (v.adjacent.isEmpty()!=true)
    			{
    		        m.adjacent = v.adjacent;
    			}

    			if (v.centroid > 0 && v.depth < min_depth)
    			{
    		        min_depth = v.depth;
    		        m.centroid = v.centroid;
    			}
    		}

    		m.depth = min_depth;

    		context.write(new LongWritable(id1), m);
    	}
    }

    

    public static class FinalMapper extends Mapper<LongWritable, Vertex, LongWritable, LongWritable>
    {
    	@Override
    	public void map(LongWritable centroid, Vertex value, Context context)throws IOException, InterruptedException
    	{
    		context.write(new LongWritable(value.centroid), new LongWritable(1));
  	}
    }

    public static class FinalReducer extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable>
    {
    	@Override
    	public void reduce(LongWritable centroid, Iterable<LongWritable> values, Context context)throws IOException, InterruptedException
    	{
    		long m=0;
    		for(LongWritable v: values)
    		{
    			 m = m+v.get();
    		}
    		context.write(centroid, new LongWritable(m));
    	}
    }

    public static void main ( String[] args ) throws Exception
    {
        Job job = Job.getInstance();
        job.setJobName("MyJob");

        //First Map_Reduce job

        job.setJarByClass(GraphPartition.class);
        job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Vertex.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Vertex.class);
		job.setMapperClass(FirstMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]+"/i0"));
        job.waitForCompletion(true);


        for ( short i = 0; i < max_depth; i++ )
        {
            BFS_depth++;
            job = Job.getInstance();

            //Second Map-Reduce 

            job.setJarByClass(GraphPartition.class);
            job.setMapOutputKeyClass(LongWritable.class);
    		job.setMapOutputValueClass(Vertex.class);
    		job.setOutputKeyClass(LongWritable.class);
    		job.setOutputValueClass(Vertex.class);
    		job.setMapperClass(SecondMapper.class);
    		job.setReducerClass(SecondReducer.class);
    		job.setInputFormatClass(SequenceFileInputFormat.class);
    		job.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(job,new Path(args[1]+"/i"+i));
            FileOutputFormat.setOutputPath(job,new Path(args[1]+"/i"+(i+1)));
            job.waitForCompletion(true);
        }


        job = Job.getInstance();
        //Final Map-Reduce job 

        job.setJarByClass(GraphPartition.class);
        job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapperClass(FinalMapper.class);
		job.setReducerClass(FinalReducer.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job,new Path(args[1]+"/i8"));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);
    }
}
