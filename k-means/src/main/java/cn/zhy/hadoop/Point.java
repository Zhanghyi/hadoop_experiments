package cn.zhy.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


// Point 实现 org.apache.hadoop.io.Writable 作为数据点坐标的数据结构
public class Point implements Writable {

    private float[] components = null;
    private int dim;
    private int numPoints; // For partial sums

    public Point() {
        this.dim = 0;
    }

    public Point(final float[] c) {
        this.set(c);
    }

    public Point(final String[] s) {
        this.set(s);
    }

    public static Point copy(final Point p) {
        Point ret = new Point(p.components);
        ret.numPoints = p.numPoints;
        return ret;
    }

    public void set(final float[] c) {
        this.components = c;
        this.dim = c.length;
        this.numPoints = 1;
    }

    public void set(final String[] s) {
        this.components = new float[s.length];
        this.dim = s.length;
        this.numPoints = 1;
        for (int i = 0; i < s.length; i++) {
            this.components[i] = Float.parseFloat(s[i]);
        }
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        this.dim = in.readInt();
        this.numPoints = in.readInt();
        this.components = new float[this.dim];

        for (int i = 0; i < this.dim; i++) {
            this.components[i] = in.readFloat();
        }
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeInt(this.dim);
        out.writeInt(this.numPoints);

        for (int i = 0; i < this.dim; i++) {
            out.writeFloat(this.components[i]);
        }
    }

    @Override
    public String toString() {
        StringBuilder point = new StringBuilder();
        for (int i = 0; i < this.dim; i++) {
            point.append(Float.toString(this.components[i]));
            if (i != dim - 1) {
                point.append(",");
            }
        }
        return point.toString();
    }

    public void sum(Point p) {
        for (int i = 0; i < this.dim; i++) {
            this.components[i] += p.components[i];
        }
        this.numPoints += p.numPoints;
    }

    //计算点之间的距离
    public float distance(Point p, int h) {
        if (h < 0) {
            h = 2;
        }

        if (h == 0) {
            float max = -1f;
            float diff = 0.0f;
            for (int i = 0; i < this.dim; i++) {
                diff = Math.abs(this.components[i] - p.components[i]);
                if (diff > max) {
                    max = diff;
                }
            }
            return max;

        } else {
            float dist = 0.0f;
            for (int i = 0; i < this.dim; i++) {
                dist += Math.pow(Math.abs(this.components[i] - p.components[i]), h);
            }
            dist = (float) Math.round(Math.pow(dist, 1f / h) * 100000) / 100000.0f;
            return dist;
        }
    }

    //计算节点平均值 用于更新中心节点
    public void average() {
        for (int i = 0; i < this.dim; i++) {
            float temp = this.components[i] / this.numPoints;
            this.components[i] = (float) Math.round(temp * 100000) / 100000.0f;
        }
        this.numPoints = 1;
    }
}