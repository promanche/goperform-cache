package ru.geosteering.goperform.cache.service;

import org.springframework.stereotype.Service;
import ru.geosteering.goperform.cache.model.CurveItem;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class CurveSimplificationService {

    public List<CurveItem> simplify(List<CurveItem> input, double epsilon) {
        List<Point> points = input.stream()
                .map(item -> new Point(item.getKey(), toDouble(item.getValue())))
                .collect(Collectors.toList());

        List<Point> simplified = douglasPeucker(points, epsilon);

        return simplified.stream()
                .map(p -> new CurveItem(p.x, p.y))
                .collect(Collectors.toList());
    }

    private double toDouble(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        throw new IllegalArgumentException("value must be a number");
    }

    private List<Point> douglasPeucker(List<Point> points, double epsilon) {
        if (points.size() < 3) return points;

        int index = 0;
        double maxDist = 0;

        Point first = points.get(0);
        Point last = points.get(points.size() - 1);

        for (int i = 1; i < points.size() - 1; i++) {
            double dist = perpendicularDistance(points.get(i), first, last);
            if (dist > maxDist) {
                index = i;
                maxDist = dist;
            }
        }

        if (maxDist > epsilon) {
            List<Point> rec1 = douglasPeucker(points.subList(0, index + 1), epsilon);
            List<Point> rec2 = douglasPeucker(points.subList(index, points.size()), epsilon);

            List<Point> result = new ArrayList<>(rec1);
            result.remove(result.size() - 1); // remove duplicate point
            result.addAll(rec2);
            return result;
        } else {
            return List.of(first, last);
        }
    }

    private double perpendicularDistance(Point p, Point start, Point end) {
        double dx = end.x - start.x;
        double dy = end.y - start.y;

        if (dx == 0 && dy == 0) {
            return Math.hypot(p.x - start.x, p.y - start.y);
        }

        double num = Math.abs(dy * p.x - dx * p.y + end.x * start.y - end.y * start.x);
        double den = Math.hypot(dx, dy);
        return num / den;
    }

    private static class Point {
        double x, y;

        Point(double x, double y) {
            this.x = x;
            this.y = y;
        }
    }
}
