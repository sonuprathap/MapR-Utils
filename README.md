# MapR-Utils

This project consists of a util written in Java which can be used to store the offsets from MapR Streams Consumer application to a MapRDB table. Since reverse scan feature is not available in MapR DB, we are storing the reverse timestamp values.
The current version is limited to Map R Distribution. 
However this can be used for other distributions also with slight modification (offset in zookeepr needs to be handled).
