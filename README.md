# nerdfunk
 ***
 Make your network resilient
 
 More information about the nerdfunk Diode can be found at https://nerdfunk.net
 
 
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
* [Nifi](http://nifi.apache.org): Version 1.14.0 or
* [Java](https://adoptopenjdk.net): Version 11


## Installation
***
The installation is quit simple
```
mvn clean install
copy nerdfunk-flow2network-processors-1.0.nar to ./lib directory of your Nifi installation
Restart Nifi
Use PutFlow2Tcp to send TCP flows and ListenTCP2flow to receive TCP flows (or UDP if needed)
```

## Collaboration
***
See nerdfunk.net for more details

## FAQs
***
Comming soon

