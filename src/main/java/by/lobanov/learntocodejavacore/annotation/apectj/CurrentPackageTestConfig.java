package by.lobanov.learntocodejavacore.annotation.apectj;

import org.springframework.context.annotation.*;

@ComponentScan(basePackages = "by.lobanov.learntocodejavacore.annotation.apectj")
@EnableAspectJAutoProxy
public class CurrentPackageTestConfig {
}
