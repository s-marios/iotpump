Repository
----------

You can obtain a copy of this project using git:

$ git clone https://github.com/s-marios/iotpump.git

Compiling from Source
---------------------

If you cloned the whole repo, to compile issue:

$ mvn compile

To run:

$ mvn exec:java

''Dist'' Setup
--------------

Compiling from source will yield a 'dist' directory in '(project_root)/target'.

This is a comiled version of the project ready for local deployment. First, copy the 'dist' folder to your desired location.

Then, to setup:

* modify config_example.properties to your liking
* copy it to config.properties

$ cp config_example.properties config.properties

Inside this 'dist' folder, run :

$ java -jar iot-pump-1.0-SNAPSHOT.jar
