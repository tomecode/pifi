# pifi
The primary idea for this project is to develop a single library that allows using scalable directed graphs of data routing, transformation, and system mediation logic. So the idea is that PIFI is will [Apache Nifi](https://nifi.apache.org/), but packaged as a single distribution library ;) 

# Requirements
- JDK 1.8
- Maven 3
- eclipse, idea,...

# Current status
- basically, the core (thread) model or engine extracted from NIFI but without provenance
- in the following class ```$pifi/src/test/java/com/pifi/demo/PocFlowDemo.java``` , is a basic simple "demo" flow, where are three activities (processors), each of them connected via a relationship, *the idea behind this flow is to read files (polling directory) and process them asynchronously*

# TODO/what's next
- extract provenance
- support routing flowfiles via multiple relationships
- unit tests!!!
- more demos + improve docs
- and more
- github actions + release
