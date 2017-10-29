# Conditional compiler flag gmake routine, from Linux.
cc-option = $(shell if $(CC) $(CFLAGS) $(1) -S -o /dev/null -xc /dev/null \
	> /dev/null 2>&1; then echo "$(1)"; else echo "$(2)"; fi;)

override CPPFLAGS := $(CPPFLAGS) -I../src -DSOCKET_DEBUG -DSOCKET_MAIN
override CFLAGS   := $(CFLAGS) -std=gnu99 -Wall -Wno-unused-function -Wno-unused-parameter $(call cc-option, -Wno-initializer-overrides,) -O2 -g -fstack-protector
override LIBS     := -lssl -lcrypto

ifeq ($(shell uname -s), Darwin)
override CFLAGS += $(CFLAGS) -Wno-deprecated-declarations
endif

all: socket

socket: socket.c ../src/dns.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -o $@ $^ $(LIBS)

socket.S: socket.c
	$(CC) $(CFLAGS) -S $(CPPFLAGS) -o $@ $^

.PHONY: clean

clean:
	rm -f socket socket.S socket.exe
	rm -fr *.dSYM

