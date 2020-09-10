
# Run the cpp_master.py script inside the container with the debug flag set
docker run -e "DEBUG=1" -it -v /home/dahlo/testarea/cpp/:/hostfs -v /home/dahlo/.kube/:/home/user/.kube  pharmbio/cpp_master python3 /hostfs/master/cpp_master.py
