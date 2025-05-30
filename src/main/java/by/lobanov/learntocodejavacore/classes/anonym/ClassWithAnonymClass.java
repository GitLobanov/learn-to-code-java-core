package by.lobanov.learntocodejavacore.classes.anonym;

import javax.swing.*;
import java.awt.event.*;

public class ClassWithAnonymClass {

    private JButton button = new JButton("Нажми на меня");

    private void clickButton () {
        // anonym class
       button.addMouseListener(new MouseListener() {
           @Override
           public void mouseClicked(MouseEvent e) {

           }

           @Override
           public void mousePressed(MouseEvent e) {

           }

           @Override
           public void mouseReleased(MouseEvent e) {

           }

           @Override
           public void mouseEntered(MouseEvent e) {

           }

           @Override
           public void mouseExited(MouseEvent e) {

           }
       }); 
    }
}
