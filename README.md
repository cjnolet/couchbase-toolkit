#Couchbase Toolkit

Couchbase is a distributed system that uses a proxy underneath memcached to add a clustering layer that will eventually converge to disk, creating what can be used as a highly available, in-memory fault-tolerant distributed document store. Couchbase is open source with a team working on a close-source build with features that may be released and better tested before the open source artifacts. 

##What does the Couchbase Toolkit provide?

While wrestling with the Couchbase Sqoop Connector, some bugs were found that didn't allow it to work properly with versions of CDH3. The actual InputFormat that pulls keys/values out of Couchbase exists in the Sqoop Connector's codebased but is hidden from the client portion of the API. Instead Sqoop as exposed, which adds an extra step for many developers wanting to directly process the keys/values from Couchbase in the map/reduce job.

This is an attempt to make public a working InputFormat that should allow parallel transfer of in-memory keys in a Couchbase bucket. Soon to come will be filtering of keys in the input format (unfortunately, the keys will still need to be transferred over the network). One goal that this project aims for is establishing true data locality for deployments where Couchbase is living directly on top of Hadoop datanodes.
