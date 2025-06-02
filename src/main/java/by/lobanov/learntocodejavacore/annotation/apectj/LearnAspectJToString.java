package by.lobanov.learntocodejavacore.annotation.apectj;

import org.springframework.beans.factory.annotation.*;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;

@SpringBootApplication(scanBasePackageClasses = CurrentPackageTestConfig.class)
public class LearnAspectJToString {

    @Autowired
    MnemosyneAspect aspect;

    public static void main(String[] args) {
        SpringApplication.run(LearnAspectJToString.class, args);
        BobUser bobUser = new BobUser("Ivan", "Ivanovich");
        System.out.println(bobUser.toString());
    }

}
