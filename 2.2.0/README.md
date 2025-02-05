# nerdfunk
 ***
 Make your network resilient
 
This is version 2.2

There are a lot of changes in 2.x This Version is not yet tested!!!

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
* [Nifi](http://nifi.apache.org): Version 2.0.0M4
* [Java](https://adoptopenjdk.net): Version 21


## Installation
***
The installation is quit simple. Please beware: currently there are no tests.
```
mvn clean install -DskipTests
copy target/nerdfunk-flow2network-processors-1.0.nar to ./lib or ./extensions directory of your Nifi installation
Restart Nifi
Use PutFlow2Tcp to send TCP flows and ListenTCP2flow to receive TCP flows
```

## Collaboration
***

## FAQs
***
Comming soon

