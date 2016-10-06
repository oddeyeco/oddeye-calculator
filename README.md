**Install**

```
git clone https://connect.netangels.net/pastor/oddeye-calculator.git
cd oddeye-calculator
mvn clean package
```

**Run**
java -jar target/OddeyeCalculator-0.1.jar -t 1h-ago -l target/classes/log4j.properties -c target/classes/config.yaml 

-st Calculate Start time by tsdb format def 1h-ago
-et Calculate End time by tsdb format def now
-l logger config file path def log4j.properties
-c progect config file path def config.yaml 
