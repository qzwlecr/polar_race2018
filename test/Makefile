all:
	g++ -std=c++11 -o test -g -I.. test.cc -L../lib -lengine -lpthread -lrt -lz
	g++ -std=c++11 -o test_with_kill -g -I.. test_with_kill.cc -L../lib -lengine -lpthread -lrt -lz

clean:
	rm test test_with_kill
	rm ralopdb*
