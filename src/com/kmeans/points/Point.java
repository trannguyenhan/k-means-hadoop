package com.kmeans.points;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class Point implements WritableComparable<Point> {
	private IntWritable x;
	private IntWritable y;

	public Point() {
		this(new IntWritable(0), new IntWritable(0));
	}

	public Point(IntWritable x, IntWritable y) {
		setX(x);
		setY(y);
	}

	public IntWritable getX() {
		return x;
	}

	public void setX(IntWritable x) {
		this.x = x;
	}

	public IntWritable getY() {
		return y;
	}

	public void setY(IntWritable y) {
		this.y = y;
	}

	@Override
	public String toString() {
		return "{" + x.get() + "," + y.get() + "}";
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Point)) {
			return false;
		}
		return this.getX() == ((Point) obj).getX() && this.getY() == ((Point) obj).getY();
	}

	@Override
	public int hashCode() {
		int ix = x.get();
		int iy = y.get();
		return 17 * ix + iy;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(x.get());
		out.writeInt(y.get());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int ix = in.readInt();
		int iy = in.readInt();
		x = new IntWritable(ix);
		y =new IntWritable(iy);
	}

	@Override
	public int compareTo(Point o) {
		// TODO Auto-generated method stub
		return 0;
	}
}
