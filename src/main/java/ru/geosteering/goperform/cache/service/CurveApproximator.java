package ru.geosteering.goperform.cache.service;

import java.math.*;
import java.util.*;

public class CurveApproximator {

    public List<Point> approximate(List<Point> points, double factor) {

        if (factor >= 1 || factor <= 0) {
            throw new IllegalArgumentException("Factor must be in the open range (0, 1)");
        }

        if (points == null || points.size() < 3) {
            throw new IllegalArgumentException("List of points must not be null and must contains more than 2 points");
        }

        List<Point> result = new ArrayList<>();
        boolean[] keepPoint = new boolean[points.size()];

        double accuracy = findAccuracy(points, factor);

        List<Integer[]> segments = new LinkedList<>();
        segments.add(new Integer[]{0, points.size() - 1});

        while (!segments.isEmpty()) {
            Integer[] indexes = segments.remove(0);
            int from = indexes[0];
            int to = indexes[1];

            keepPoint[from] = true;
            keepPoint[to] = true;

            int maxIndex = 0;
            double maxDist = 0;

            for (int i = from + 1; i < to; i++) {
                double dist = dist(points.get(from), points.get(to), points.get(i));
                if (dist > maxDist) {
                    maxIndex = i;
                    maxDist = dist;
                }
            }

            if (maxDist >= accuracy) {
                keepPoint[maxIndex] = true;
                segments.add(new Integer[]{from, maxIndex});
                segments.add(new Integer[]{maxIndex, to});
            }
        }

        for (int i = 0; i < points.size(); i++) {
            if (keepPoint[i]) {
                result.add(points.get(i));
            }
        }

        return result;
    }

    private double findAccuracy(List<Point> points, double factor) {

        double maxDist = points.stream()
                .mapToDouble(p -> dist(points.get(0), points.get(points.size() - 1), p))
                .max()
                .orElse(0);

        return maxDist * factor;
    }

    private double dist(Point first, Point second, Point point) {
        BigDecimal x0 = new BigDecimal(point.x).setScale(3, RoundingMode.HALF_UP);
        BigDecimal y0 = new BigDecimal(point.y).setScale(3, RoundingMode.HALF_UP);

        BigDecimal x1 = new BigDecimal(first.x).setScale(3, RoundingMode.HALF_UP);
        BigDecimal y1 = new BigDecimal(first.y).setScale(3, RoundingMode.HALF_UP);

        BigDecimal x2 = new BigDecimal(second.x).setScale(3, RoundingMode.HALF_UP);
        BigDecimal y2 = new BigDecimal(second.y).setScale(3, RoundingMode.HALF_UP);

        BigDecimal A = y2.add(y1.negate());
        BigDecimal B = x2.add(x1.negate());
        BigDecimal C = x2.multiply(y1).add(y2.multiply(x1).negate());

        return ((((A.multiply(x0)).add((B.multiply(y0).negate())).add(C)).abs())
                .divide((((A.pow(2)).add((B.pow(2)))).sqrt(MathContext.DECIMAL128)), MathContext.DECIMAL128))
                .setScale(3, RoundingMode.HALF_UP).stripTrailingZeros()
                .doubleValue();
    }

    private static class Point {
        double x;
        double y;

        public Point(double x, double y) {
            this.x = x;
            this.y = y;
        }
    }
}
