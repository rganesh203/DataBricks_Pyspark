#Hadoop 
#first of all the services active use the command
jps
#every hadoop command hdfs with -
hadoop fs #any path file
hdfs dfs  #only hadoop file
hadoop fs -ls #check the list of files
hadoop fs -ls g #path 
hadoop fs -mkdir ganesh #make a directory
hadoop fs -touchz ap.txt #create new file
hadoop fs -copyFromLocal ap.txt ganesh #path #copy from local
hadoop fs -cat f1.txt #read the content from the file
hadoop fs -copyToLocal ap.txt ~/Desktop/ #copy to the local
cd Desktop/
ls
cat ap.txt
hadoop fs -cp path1/ap.txt path2 #send file hdfs to hdfs
hadoop fs -mv path1/ap.txt path2 #move file hdfs to hdfs
hadoop fs -du directory # disk usage of each file
hadoop fs -dus directory #total size of directory
#open new window type localhost:port no.

#second part:
hadoop fs -test -d directory #
echo $? #test with d
hadoop fs -test -e directory # exist or not in my hdfs
echo $? 
hadoop fs -test -f directory # file or not
echo $?
hadoop fs -test -z directory # zero byte file or not
echo $?
hadoop fs -moveFromLocal az.txt path/ # move local from hdfs
cat az.txt
hadoop fs -getmerge -nl path/file1 path/file2 ~/Desktop/mergeresult.txt #merge two files
cat Desktop/mergeresult
gedit a.txt #create new file and open
hadoop fs -appendToFile q1.txt q2.txt q3.txt # appendfile
hadoop fs -checksum destinatiion/xyz.txt #any character changes using
hadoop fsck - / #check the staus of hadoop location
hadoop fs -count destinatiion/ #count directory
hadoop fs -rm destinatiion/xyz #delete file
hadoop fs -chgrp armit jathin/a2.txt #change the group of file 
hadoop fs -stat %b destinatiion/xyz1.txt
hadoop fs -stat %g destinatiion/xyz1.txt
hadoop fs -stat %r destinatiion/xyz1.txt #replication factor
hadoop fs -stat %u destinatiion/xyz1.txt # username
hadoop fs -stat %y destinatiion/xyz1.txt #when the file got modified

#Third Part:
hadoop fs -usage mkdir #syntax of any command
hadoop fs -help cat # description
hadoop fs -head destinatiion/xyz1.txt #1kb of first file data
hadoop fs -tail destinatiion/xyz1.txt #1kb of last file data
hadoop fs -expunge # getread of trach files
hadoop fs -chown amrit:amrit destinatiion/xyz1.txt #change the ownership of 
hadoop fs -chmod 777 destinatiion/xyz1.txt #user, group, others
hadoop fs -setrep -w 3 destinatiion/xyz1.txt #change the replication factor
hadoop fs -truncate -w 100 destinatiion/xyz1.txt #reduce the size of log files, get readoff large file
hadoop fs -setfattr -n 'user.ap' -v this is dummy filedestinatiion/xyz1.txt
hadoop fs -getfattr -d destinatiion/xyz1.txt











