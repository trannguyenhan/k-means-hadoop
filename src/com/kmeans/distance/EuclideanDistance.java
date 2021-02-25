package com.kmeans.distance;

import com.kmeans.points.Point;

public class EuclideanDistance implements Distance{

	@Override
	public double calculate(Point p1, Point p2) {
		return Math.sqrt(
				(p1.getX() - p2.getX()) * (p1.getX() - p2.getX()) + (p1.getY() - p2.getY()) * (p1.getY() - p2.getY()));
	}
}
