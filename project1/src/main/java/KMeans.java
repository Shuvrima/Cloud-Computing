import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.net.URI;




class Point implements WritableComparable<Point>
{
	public double x; //taken from pseudocode
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
	public int compareTo(Point p) //to find if points are same or not 
	{
		if(p.x==this.x)
		{
			return (int)(p.y-this.y);
		}

		else
		{
			return (int)(p.x-this.x);
		}
	}
	public String toString() //processed as strings for delivering
	{
		return(this.x+","+this.y);
	}

}
public class KMeans
{
	static Vector<Point> centroids= new Vector<Point>(100); //from pseudocode

	public static class MyMapper extends Mapper<Object,Text,Point,Point> 
	{

		public void setup(Context context) throws IOException, InterruptedException
		{
			
			URI[] paths = context.getCacheFiles();
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(paths[0]))));
			String clines; 
			
			//reading the centroids from centroids.txt
			
			for(int i=0;i<100;i++)
			{
				clines=reader.readLine();
				String[] one=clines.split(",");
				String m = one[0];
				String n = one[1];

				Point MyP= new Point();
				MyP.x=Double.parseDouble(m);
				MyP.y=Double.parseDouble(n);

				centroids.addElement(MyP);
			}	
		}

		@Override		
		public void map ( Object key, Text value, Context context )throws IOException, InterruptedException 
		{
			Point x1= new Point();
			Point x2= new Point();
			Point c= new Point();
			Point o= new Point();

			// reading the input points from the file
			Scanner s = new Scanner(value.toString()).useDelimiter(",");
			x1.x=s.nextDouble();
			x1.y=s.nextDouble();

			double min= Double.MAX_VALUE , distance;
			//calculating the nearest centroid for each point
			for(int z=0; z<100; z++)
			{
				x2=centroids.get(z);

				distance=Math.sqrt(((x1.x-x2.x)*(x1.x-x2.x))+((x1.y-x2.y)*(x1.y-x2.y)));
//using euclidean distance
				if(distance<=min)
				{
					min=distance; //updating the closest
             				o.x= x1.x;
					o.y= x1.y;
					c.x= x2.x;
					c.y= x2.y;

				}
			}
			context.write(c,o);
			s.close();
		}


	}

	public static class MyReducer extends Reducer<Point,Point,Point,Object> 
	{
		public void reduce ( Point c, Iterable<Point> Points, Context context )throws IOException, InterruptedException 
		{
			
			//calculating the new centroids according to pseudocode
			int count=0;
			double sx=0.0, sy=0.0;

			for (Point p: Points) 
			{
				count++;
				sx = sx + p.x;
				sy = sy + p.y;
			}

			c.x= sx/count;
			c.y= sy/count;
 
			context.write(c,null); //emit
		}
	}



	public static void main ( String[] args ) throws Exception 
	{
		Job job = Job.getInstance();
		job.setJobName("Job1"); //using code from simple example to use job class
		job.setJarByClass(KMeans.class);
		job.setOutputKeyClass(Point.class);
		job.setOutputValueClass(Point.class);
		job.setMapOutputKeyClass(Point.class);
		job.setMapOutputValueClass(Point.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.addCacheFile(new URI(args[1]));
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[2]));
		job.waitForCompletion(true);

	}
}

