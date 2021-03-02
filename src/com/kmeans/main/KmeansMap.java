package com.kmeans.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;

import com.kmeans.distance.Distance;
import com.kmeans.distance.EuclideanDistance;
import com.kmeans.points.Point;

public class KmeansMap extends Mapper<Point, Point, Point, Point> {
	private static List<Point> listCenter = new ArrayList<>();
	
	/* Method setup, we will get k centers from sequence file cen.seq
	 * */
	@SuppressWarnings("resource")
	@Override
	protected void setup(Mapper<Point, Point, Point, Point>.Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Path centers = new Path(conf.get("pathCenter"));
		
		SequenceFile.Reader reader = null;
		reader = new SequenceFile.Reader(conf, Reader.file(centers), Reader.bufferSize(4096));
		
		Point key = new Point();
		Point value = new Point();
		while(reader.next(key, value)) {
			listCenter.add(key);
		}		
		
		reader.close();
	}

	@Override
	protected void map(Point key, Point value, Mapper<Point, Point, Point, Point>.Context context)
			throws IOException, InterruptedException {
		System.out.print("number of center : " + listCenter.size() + " => ");
		System.out.print("mapper : ");
		
		Point nearest = null;
		Distance distance = new EuclideanDistance();
		double minDistance = Double.MAX_VALUE;
		
		for(Point c : listCenter) {
			double tmpDistance = distance.calculate(c, value);
			if(tmpDistance < minDistance) {
				minDistance = tmpDistance;
				nearest = c;
			}
		}
		context.write(nearest, value);
		
		System.out.print("(center)" + nearest + " | (point)" + value + "\n");
	}
}
