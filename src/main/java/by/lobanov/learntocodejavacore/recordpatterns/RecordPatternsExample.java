package by.lobanov.learntocodejavacore.recordpatterns;

import java.awt.*;

public class RecordPatternsExample {

    record Point2D(int x, int y) {}
    record ColoredPoint(Point2D point, Color color) {}

    public static void main(String[] args) {
        Point2D point2D = new Point2D(3, 7);
        ColoredPoint coloredPoint = new ColoredPoint(point2D, Color.BLUE);
        if (coloredPoint instanceof ColoredPoint(Point2D(int x, int y), Color c)) {
            System.out.println("Point coordinates: (" + x + ", " + y + ")");
            System.out.println("Color: " + c);
        }
    }
}
