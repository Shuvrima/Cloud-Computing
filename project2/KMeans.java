import java.io.*;
import java.util.Hashtable;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.net.URI;


//Using pseudocode

class Point implements WritableComparable<Point>
{
	public double x;
	public double y;


	public void write(DataOutput out) throws IOException 
	{
		out.writeDouble(x);
		out.writeDouble(y);

	}
	public void readFields(DataInput in) throws IOException
	{
		x=in.readDouble();
		y=in.readDouble();

	}
	public int compareTo(Point o) 
	{
		if(o.x==this.x)
		{
			return (int)(o.y-this.y);
		}

		else
		{
			return (int)(o.x-this.x);
		}
	}
	public String toString()
	{
		return(this.x+","+this.y);
	}

}

class Avg implements Writable
{
	public double sumX;
	public double sumY;
	public long count;

	public void write(DataOutput out) throws IOException 
	{
		out.writeDouble(sumX);
		out.writeDouble(sumY);
		out.writeLong(count);
		
	}

	public void readFields(DataInput in) throws IOException 
	{
		sumX=in.readDouble();
		sumY=in.readDouble();
		count=in.readLong();
		
	}
}

public class KMeans
{
	static Vector<Point> centroids= new Vector<Point>(100);
	static Hashtable<Point,Avg> table= new Hashtable<Point,Avg>(100);

	public static class Avg_Mapper extends Mapper<Object,Text,Point,Avg> 
	{
		public void setup(Context context) throws IOException, InterruptedException
		{
			URI[] paths = context.getCacheFiles(); //using code given by professor
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(paths[0]))));
			int i=0;
			while(i!=100)
			{
				String line=reader.readLine();
				String[] single=line.split(",");
				String first = single[0];
				String second = single[1];

				Point P= new Point();
				P.x=Double.parseDouble(first);
				P.y=Double.parseDouble(second);
				centroids.addElement(P);
				
				i++;
				
			}
			for(int j=0; j<100; j++)
			{
				Avg pA= new Avg();
				pA.sumX=0;
				pA.sumY=0;
				pA.count=0;
				
				table.put(centroids.get(j),pA);
			}
		}
		
		public void map(Object key, Text value, Context context)throws IOException, InterruptedException
		{
			Point P1= new Point();
			Point P2= new Point();
			
			Scanner sc=new Scanner(value.toString()).useDelimiter(",");
			double x=sc.nextDouble();
			double y=sc.nextDouble();
			
			P1.x=x;
			P1.y=y;
			double min= Double.MAX_VALUE , distance;
			int position=0;
			
			for(int k=0; k<100; k++)
			{
				P2=centroids.get(k);

				distance=(((P1.x-P2.x)*(P1.x-P2.x))+((P1.y-P2.y)*(P1.y-P2.y)));

				if(distance<=min)
				{
					min=distance;
					position=k;
				}
			}
			
			if(table.get(centroids.get(position)).count==0)
			{
				Avg ag=new Avg();
				ag.sumX=x;
				ag.sumY=y;
				ag.count=1;
				table.put(centroids.get(position),ag);
			}
			else
			{
				Avg ag=new Avg();
				
				ag.sumX=x+table.get(centroids.get(position)).sumX;
				ag.sumY=y+table.get(centroids.get(position)).sumY;
				ag.count=1+table.get(centroids.get(position)).count;
				table.put(centroids.get(position),ag);
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			Set<Point> keys=table.keySet();
			for(Point c:keys)
			{
				context.write(c, table.get(c));
			}
		}
		
	}


	public static class Avg_Reducer extends Reducer<Point,Avg,Point,Object> 
	{
		public void reduce(Point c, Iterable<Avg> avgs, Context context)throws IOException, InterruptedException
		{
			long count=0;
			double sx=0.0, sy=0.0;
			for(Avg a: avgs)
			{
				count=count+a.count;
				sx=sx+a.sumX;
				sy=sy+a.sumY; //From pseudocode
			}
			c.x=sx/count;
			c.y=sy/count;
			context.write(c, null); //emit
		}
		
	}
	



	public static void main ( String[] args ) throws Exception 
	{ //Using jobber from example codes given
		Job job = Job.getInstance();
		job.setJobName("MyJob");
		job.setJarByClass(KMeans.class);
		job.setOutputKeyClass(Point.class);
		job.setOutputValueClass(Point.class);
		job.setMapOutputKeyClass(Point.class);
		job.setMapOutputValueClass(Avg.class);
		job.setMapperClass(Avg_Mapper.class);
		job.setReducerClass(Avg_Reducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.addCacheFile(new URI(args[1]));
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[2]));
		job.waitForCompletion(true);

	}
}

