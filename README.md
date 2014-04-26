#Couchbase Toolkit

Couchbase is a distributed system that uses a proxy underneath memcached to add a clustering layer that will eventually converge to disk, creating what can be used as a highly available, in-memory fault-tolerant distributed document store. Couchbase is open source with a team working on a close-source build with features that may be released and better tested before the open source artifacts. 

##What does the Couchbase Toolkit provide?

While wrestling with the Couchbase Sqoop Connector, some bugs were found that didn't allow it to work properly with versions of CDH3. The Couchbase team also purposely hid the actual InputFormat that pulls keys/values out of couchbase from the client portion of the API. Instead Sqoop as exposed, which adds an extra step for many developers wanting to directly process the keys/values from Couchbase in the map/reduce job.
