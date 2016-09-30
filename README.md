**Install**

```
git clone https://connect.netangels.net/pastor/oddeye-calculator.git
cd oddeye-calculator
mvn clean package
```

**Run**
java -jar target/OddeyeCalculator-0.1.jar -t 1h-ago -l target/classes/log4j.properties -c target/classes/config.yaml 

-t Calculate time by tsdb format
-l logger config file path 
-c progect config file path 
