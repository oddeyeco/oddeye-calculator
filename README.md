**Install**

```
git clone https://github.com/oddeyeco/oddeye-calculator.git
cd oddeye-calculator
mvn clean package
```

**Run**
```
java -jar target/OddeyeCalculator-0.1.jar -t 1h-ago -l target/classes/log4j.properties -c target/classes/config.yaml 
```
-t Calculate time by tsdb format
-l logger config file path 
-c progect config file path 

Set hourly cron job to run calculator . 

Example executor shell script ```calc.sh``` :
```
#!/bin/bash

LOCKFILE=/tmp/.calculator.lock

if  [ -f $LOCKFILE ]
 then 
  echo 'Lock file exists, exitting' 
else
  touch $LOCKFILE
  cd /opt/oddeye-calculator 
  /srv/java/bin/java  -Xmx2g -Xms2g -jar /opt/oddeye-calculator/oe-rules.jar -c /opt/oddeye-calculator/config.yaml
  rm $LOCKFILE
fi
```
Crontab :

```@hourly /path/to/calc.sh```

