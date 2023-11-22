steps to interact with the container
1) build the image
2) run docker-compose up
3) in vs code under the docker tab, click and attach to shell
4) navigate in the shell to /usr/src/app
- from there, any file i create locally in the app sub folder will be bridged there 
- e.g., if i create a file in the app subfolder locally called "test1.py", i can then in the container vs shell run 
        /usr/src/app python test1.py