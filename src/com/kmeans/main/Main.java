package com.kmeans.main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import com.kmeans.distance.Distance;
import com.kmeans.distance.EuclideanDistance;
import com.kmeans.points.Point;

public class Main {
	private static int k = 3; // 3 clusters
	private static int numberOfPoints;
	private static List<Point> listPoints = new ArrayList<>();
	private static List<Point> listCentroid = new ArrayList<>();
	private static Map<Point, Point> mapPointInCentroid = new TreeMap<>();
	
	private static void getDataInput() throws NumberFormatException, IOException {
		@SuppressWarnings("resource")
		BufferedReader buffer = new BufferedReader(new FileReader("data/input.txt"));

		// get number of points
		numberOfPoints = Integer.parseInt(buffer.readLine());

		// get list of points
		listPoints = new ArrayList<>();
		String line = buffer.readLine();
		while (line != null) {
			String tmpString[] = line.split(" ");
			Integer tmp0 = Integer.parseInt(tmpString[0]);
			Integer tmp1 = Integer.parseInt(tmpString[1]);
			Point tmpPoint = new Point(tmp0, tmp1);

			listPoints.add(tmpPoint);

			line = buffer.readLine();
		}
	}

	private static void randomCentroid() {
		Random rd = new Random();
		int numberOfClusters = k;
		
		while(numberOfClusters > 0) {
			int i = rd.nextInt(numberOfPoints);
			listCentroid.add(listPoints.get(i));
			numberOfClusters--;
		}
		
		for(Point point : listPoints) {
			mapPointInCentroid.put(point, listCentroid.get(0));
		}
	}
	
	private static Point nearestCentroid(Point point) {
		Distance distance = new EuclideanDistance();
		double minDistance = Double.MAX_VALUE;
		int indexMinDistance = 0;
		
		for(int i = 0; i<listCentroid.size(); i++) {
			double distancePointToCentroid = distance.calculate(point, listCentroid.get(i)); 
			if( distancePointToCentroid < minDistance) {
				minDistance = distancePointToCentroid;
				indexMinDistance = i;
			}
		}
		
		return listCentroid.get(indexMinDistance);
	}
	
	public static void main(String[] args) throws NumberFormatException, IOException {
		getDataInput();
		randomCentroid();
		
		System.out.println(listCentroid.get(0));
	}
}
