# DBStream
DBStream
read database increase log to local ,and analyse increase data ,then store it as an timeline database.
user can use it to consume increase data directly,just like kafka.also user can use it to sync  increase data from database to database.
now only support mysql.we will support oracle and postgresql late.

compile:
in linux plateform
at first get gflags and glog from github for our log used glog
  https://github.com/gflags/gflags
  https://github.com/google/glog
  compile those two ,and make install.
then get mysql 8 from mysql ,because we need mysql client to connect mysql.
  https://dev.mysql.com/downloads/mysql/, download mysql client rpm package and install.
next goto lz4 dir in DBStream
  directly do make in linux
finally goto DBStream
  cmake . 
  make
  
