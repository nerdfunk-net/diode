# nerdfunk
 ***
Make your network resilient
 
The nerdfunk Nifi Processors are used to transfer any data between separated networks. Use PuFlow2TCP to send data and ListenTCP2flow to receive data.  
 
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
* [Nifi](http://nifi.apache.org): Version 1.13.2 or
* [Nifi](http://nifi.apache.org): Version 1.14.0 or newer and
* [Java](https://adoptopenjdk.net): Version 11


## Installation
***
There are two versions. Version 1.13.2 is the 'old' one that is based on Nifi 1.13.2. This version runs for a longer time now and is tested.
The newer versions (1.14.0 and above) are based on the event-transport code (netty). These version were not tested in a procution environment yet.

The installation is quit simple
```
mvn clean install
copy nerdfunk-flow2network-processors-1.0.nar to ./lib directory of your Nifi installation (eithe 1.13.2 oder 1.14.0)
Restart Nifi
Use PutFlow2Tcp to send TCP flows and ListenTCP2flow to receive TCP flows (or UDP if needed)
```

## Collaboration
***
See nerdfunk.net for more details

## FAQs
***
Comming soon

