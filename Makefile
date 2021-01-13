GCC = g++
CFLAGS = -O2 -w -std=c++14 -pthread -fopenmp
SSEFLAGS =   -march=native
FILES = exclusive_hash

all: $(FILES) 

exclusive_hash: main.cpp
	$(GCC) $(CFLAGS) $(SSEFLAGS) -o exclusive_hash main.cpp

.PHONY: clean
clean:
	-rm -f exclusive_hash $(objs) *.out
