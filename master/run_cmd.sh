# https://github.com/moby/moby/issues/9448
docker run -it -v /home/dahlo/testarea/cpp/master/:/data:ro -v /home/dahlo/.kube:/home/user/.kube:ro  --security-opt apparmor:unconfined --cap-add SYS_ADMIN --device=/dev/fuse  dahlo/cpp_runner
