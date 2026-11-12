CC ?= gcc
CFLAGS ?= -std=c11 -Wall -Wextra -Wpedantic -Werror -O2
TARGET = mq_router

.PHONY: all run test demo asan clean

all: $(TARGET)

$(TARGET): mq_router.c
	$(CC) $(CFLAGS) -o $@ $<

run: $(TARGET)
	./$(TARGET) router

test: $(TARGET)
	./$(TARGET) test

demo: $(TARGET)
	sh ./demo.sh

asan: mq_router.c
	$(CC) -std=c11 -Wall -Wextra -Wpedantic -Werror -O1 -g \
		-fsanitize=address,undefined -fno-omit-frame-pointer \
		-o mq_router_asan mq_router.c

clean:
	rm -f $(TARGET) mq_router_asan router.log queue-full.log \
		permission.log removed.log unavailable.log crash-client.log slow-client.log