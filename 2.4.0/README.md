# nerdfunk
 ***
 Make your network resilient
 
This is version 2.4

There are a lot of changes in 2.x 
Version 2.2 was tested, version 2.4 is new and not yet tested.

## Table of Contents
1. [General Info](#general-info)
2. [Technologies](#technologies)
3. [Installation](#installation)
4. [Collaboration](#collaboration)
5. [FAQs](#faqs)

## General Info
***
nerdfunk is a nifi based Diode to transfer data to a separate network.


## Technologies
***
Nerdfunk is based on nifi, an open source flow management system (http://nifi.apache.org)

A list of technologies used within the project:
* [Nifi](http://nifi.apache.org): Version 2.4.0
* [Java](https://adoptopenjdk.net): Version 21


## Installation
***
The installation is quit simple. Please beware: currently there are no tests.
```
mvn clean install -DskipTests
copy target/nerdfunk-flow2network-processors-2.4.nar to ./extensions directory of your Nifi installation
Restart Nifi
Use PutFlow2Tcp to send TCP flows and ListenTCP2flow to receive TCP flows
```

## Collaboration
***

## FAQs
***
Comming soon

