# Postgresql docker deployment

Use docker to deploy multiple Postgresql servers.

## Getting Started

### Prerequisites

We need to [install docker engine](https://docs.docker.com/install/) on your local machine firstly.


### Get the offical image of Postgresql from Docker Hub

Pull the latest version image of Postgresql.

```
[user@localmachine]$ docker pull postgres:latest
[output]:
latest: Pulling from library/postgres
68ced04f60ab: Pull complete 
59f4081d08e6: Pull complete 
74fc17f00df0: Pull complete 
8e5e30d57895: Pull complete 
a1fd179b16c6: Pull complete 
7496d9eb4150: Pull complete 
0328931819fd: Pull complete 
8acde85a664a: Pull complete 
38e831e7d2d3: Pull complete 
582b4ba3b134: Pull complete 
cbf69ccc1db5: Pull complete 
1e1f3255b2e0: Pull complete 
c1c0cedd64ec: Pull complete 
6adde56874ed: Pull complete 
Digest: sha256:110d3325db02daa6e1541fdd37725fcbecb7d51411229d922562f208c51d35cc
Status: Downloaded newer image for postgres:latest
docker.io/library/postgres:latest
```

Check the installed images, we could see the image names with their ID numbers.

```
[user@localmachine]$ docker images
[output]:
REPOSITORY          TAG                 IMAGE ID            CREATED             SIZE
postgres            latest              73119b8892f9        10 days ago         314MB
hello-world         latest              e38bc07ac18e        23 months ago       1.85kB
```


## Create and run a container instance


### Start an instance

```
[user@localmachine]$ docker run --name dockerPG1 -d -e POSTGRES_PASSWORD=YOURPASSWORD -p 5432:5432 postgres:latest
```

1. run: create and start an instance；

2. -–name: set container alias；

3. -e POSTGRES_PASSWORD=: set environment variable of password for the postgresql server. You may also use "POSTGRES_HOST_AUTH_METHOD=trust" to allow all connections without a password.

4. -p PORT_A:PORT_B: port mapping, A is the port on local machine, B is the exposed port on docker instance.

5. -d: run containers in the background, with printing new container name.

6. To create another container instance, execute the command above again with a different PORT_A and name.

### Check the current running docker instance state

If the output info include the postgresql instance, it successfully runs in the background.

```
[user@localmachine]$ docker ps
[output]:
CONTAINER ID        IMAGE               COMMAND                  CREATED              STATUS              PORTS                    NAMES
70ba593db9ba        postgres            "docker-entrypoint.s…"   About a minute ago   Up About a minute   0.0.0.0:5432->5432/tcp   elastic_herschel
```

## Connect to the postgresql server 

### from the outside of the container instance
You can use the postgresql driver to connect (**Username: postgres | Password: YOURPASSWORD**) or execute the shell command below.

```
[user@localmachine]$ psql -U postgres -h localhost -p 5432

```

### from the inside of the container instance

```
[user@localmachine]$ docker exec -it CONTAINER_ID /bin/bash
[root@70ba593db9ba:/]# su -u postgres
[postgres@70ba593db9ba:/]# su -u postgres
[postgres@70ba593db9ba:/]# psql -d postgres

[output]:

psql (12.2 (Debian 12.2-2.pgdg100+1))
Type "help" for help.

postgres=# 


```
You may get the Container ID by **docker ps** command


## Stop the instance

```
[user@localmachine]$ docker stop CONTAINER_ID

```

