# distranscode
Experimental distributed transcoding

Docker containers to maintiain an mp3 version of a Flac media tree. Rabbit MQ container provides pub/sub to distribute tasks from the tasker container to multiple worker containers. Runs on x86 and Rasbperry Pi. Assumes workers have mounted the an nfs4 root outside of docker container with subdirs for flac inputi (albums), mp3 output (mp3), python scripts (hpc). https://github.com/gondor/docker-volume-netshare makes this easier

# RabbitMQ

docker run --name rabbitmq --net=host rabbitmq:3-management

# Tasker
sudo docker run -v <b>&lt;script dir&gt;</b>:/hpc -v <b>&lt;flac input dir&gt;</b>:/albums:ro -v <b>&lt;mp3 output dir&gt;</b>:/mp3 --net=host -it mp3-tasker python /hpc/flac2mp3tasker2.py

# Worker
sudo docker run  -v <b>&lt;nfs root mount&gt;</b>:/muzak --net=host -it mp3-worker_arm python /muzak/hpc/flac2mp3worker2.py

# TODO 

share config with Zookeeper?
sanitise config
replace nfs (but without adding extra file copies)
use python modules for transcode instead of system call
