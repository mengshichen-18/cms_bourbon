rm expres.txt

g++ -o Main singletest.cpp ../build/libleveldb.a -lpthread -lsnappy -I ./include/ -g

./Main 8 20 1000000 7 0
./Main 8 20 1000000 7 20
./Main 8 20 1000000 7 40
./Main 8 20 1000000 7 60
./Main 8 20 1000000 7 80
./Main 8 20 1000000 7 100
