package com.kmeans.distance;

import com.kmeans.points.Point;

public interface Distance {
	double calculate(Point p1, Point p2);
}
