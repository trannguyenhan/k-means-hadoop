package com.kmeans.points;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;

public class PointService {
	public static List<Point> getListPoints(String path) throws IOException {
		@SuppressWarnings({ "unchecked", "rawtypes" })
		List<Point> listPoints = new ArrayList();
		@SuppressWarnings("resource")
		BufferedReader buffer = new BufferedReader(new FileReader(path));

		// get list of points
		String line = buffer.readLine();
		while (line != null) {
			String tmpString[] = line.split(" ");
			Integer tmp0 = Integer.parseInt(tmpString[0]);
			Integer tmp1 = Integer.parseInt(tmpString[1]);
			Point tmpPoint = new Point(new IntWritable(tmp0), new IntWritable(tmp1));

			listPoints.add(tmpPoint);
			line = buffer.readLine();
		}
		
		return listPoints;
	}
}
