package by.lobanov.learntocodejavacore.recordpatterns;

import java.awt.*;

public class RecordPatternsSwitchExample {

    record Point2D(int x, int y) {}
    sealed interface Shape permits Circle, Rectangle {}
    record Circle(Point2D center, double radius) implements Shape {}
    record Rectangle(Point2D topLeft, Point2D bottomRight) implements Shape {}

    public static void main(String[] args) {
        Point2D point2DLeft = new Point2D(3, 7);
        Point2D point2DRight = new Point2D(3, 7);
        Rectangle rectangle = new Rectangle(point2DLeft, point2DRight);
        describeShape(rectangle);
    }

    static void describeShape(Shape shape) {
        switch (shape) {
            case Circle(Point2D(int x, int y), double r) ->
                    System.out.println("Circle with center at (" + x + ", " + y + ") and radius " + r);

            case Rectangle(Point2D(int x1, int y1), Point2D(int x2, int y2)) ->
                    System.out.println("Rectangle from (" + x1 + ", " + y1 + ") to (" + x2 + ", " + y2 + ")");
        }
    }


}
