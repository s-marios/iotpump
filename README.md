Dist Setup
----------

You have in your hands a comiled version of the project. You are now in the "dist" (i.e. distribution) folder that was generated from compilation.

To setup:

* modify config_example.properties to your liking
* copy it to config.properties
    * $ cp config_example.properties config.properties

To run (from inside this "dist" folder):

* $ java -jar iot-pump-1.0-SNAPSHOT.jar

Setup With Source
-----------------

If you cloned the whole repo, to compile issue:

$ mvn compile

To run:

$ mvn exec:java
