OStore (Ordered-Store) is a new software system for 
storing and processing big data.

## OStore Distinguishing Characteristics
   1. At its core is a NoSQL database.
   2. It is written in Scala and makes extensive use of Akka 
      actors.
   3. Keys are stored in sorted order rather than hashed.
   4. It supports a continuous incremental map-reduce.

## Current State
OStore is curently in the early stages of development and not
suitable for production use. It contains bugs and is missing lots
of features. Also APIs will probably change in
non-upward compatible ways. Error detection and recovery
is partially missing.
The overall system has not been fully tuned for performace.

## License
The OStore code is licensed under the Apache 2.0 license.

## Documentation
1. [Wiki](ostore/wiki)
1. ScalaDoc link

## Contact
If you are interested in doing serious testing of OStore
or contributing to OStore as a developer please send email
to 

[nestor@persist.com](mailto:nestor@persist.com)
