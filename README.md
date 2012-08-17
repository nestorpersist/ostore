1.0 Introduction

[Wiki](ostore/wiki)

OStore is a new comprehensive software system for 
storing and processing big data.

OStore has four distinguishing characteristics
   1. At its core is a NoSQL database
   2. It is written in Scala and makes extensive use of Akka 
      actors.
   3. Keys are stored in sorted order rather than hashed.
   4. It provides a continuous map-reduce engine.

OStore is curently in the early stages of development and not
suitable for serious use. It contains bugs and is missing lots
of features. Also APIs will probably change in
non-upward compatible ways. Error checking is partially missing
and it has not been tuned for performace.

The <b>OStore</b> code is licensed under the Apache 2.0 license.
