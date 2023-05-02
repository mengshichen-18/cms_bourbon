rm segexpres.txt

g++ -o Main segtest.cpp ../build/libleveldb.a -lpthread -lsnappy -I ./include/ -g

./Main 2 20 1000000 7 0 10
./Main 2 20 1000000 7 20 10
./Main 2 20 1000000 7 40 10
./Main 2 20 1000000 7 60 10
./Main 2 20 1000000 7 80 10
./Main 2 20 1000000 7 100 10
