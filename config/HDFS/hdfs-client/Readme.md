# HDFS-client 

## Working with HDFS through HDFS-client pod

Locate or create an hdfs-client pod in your namespace. Pod name: hdfs-client-<user>-\<hash>-\<hash>.

There are two ways of wokring with hdfs through hdfs-client pod. First one - access the pod bash shell and work from there:
```
kubectl exec -it -n <your-namespace> hdfs-client-<user>-<hash> -- bash
```

Then we get into the pod shell and execute hdfs commands from there.

Second option - use kubectl exec command with non-interactive mode. Overall syntax:
```
kubectl exec -n <your-namespace> <hdfs-client-pod-name> -- <command>
```

To check the structure of hdfs command tool simply enter `hdfs` as command:
```
kubectl exec -n <your-namespace> hdfs-client-<user>-<hash> -- hdfs
```



## Browsing directories

List all directories in root folder of hdfs:

```
kubectl exec -n <your-namespace> hdfs-client-<user>-<hash> -- hdfs dfs -ls /
```

To check content of any other folder replace `/` with path e.g. `/data/`:
```
kubectl exec -n <namespace> hdfs-client-<user>-<hash> -- hdfs dfs -ls /data/
```

Create hdfs directory:
```
kubectl exec -n <namespace> hdfs-client-<user>-<hash> -- hdfs dfs -mkdir /home/test-user/testdir/
```



## Working with files

To copy files from host system to Hadoop your need to copy it into pod first, then execute hdfs command.

Copy file from local system to hdfs-client pod:
```
kubectl cp -n <namespace> ./hdfs-client/test.txt hdfs-client-<user>-<hash>:test.txt
```

Check if file was properly copied:
```
kubectl exec -n <namespace> hdfs-client-<user>-<hash> -- cat test.txt
```

Put file from pod filesystem into hdfs:
```
kubectl exec -n <namespace> hdfs-client-<user>-<hash> -- hdfs dfs -put test.txt /home/test-user
```

Check if file present on hdfs:
```
kubectl exec -n <namespace> hdfs-client-<user>-<hash> -- hdfs dfs -ls /home/test-user
```

Another option to copy files from local into hdfs right away, without kubectl cp command (it is much slower):
```
cat test.txt | xargs -I {} kubectl exec -n <namespace> hdfs-client-<user>-<hash> -- bash -c "echo {} | hdfs dfs -appendToFile - /home/test-user/test4.txt"
```

Delete file from hdfs:
```
kubectl exec -n <namespace> hdfs-client-<user>-<hash> -- hdfs dfs -rm /home/test-user/test.txt
```

Copy file from one hdfs directory to another:
```
kubectl exec -n <namespace> hdfs-client-<user>-<hash> -- hdfs dfs -cp <source> <destination>
```

Donwload file from hdfs to local:
```
kubectl exec -n <namespace> hdfs-client-<user>-<hash> -- hdfs dfs -get /home/user-test/test4.txt /test4.txt
```

Then copy file from pod to local filesystem if needed.


