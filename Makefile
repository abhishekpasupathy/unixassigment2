CC ?= gcc
CFLAGS ?= -std=c11 -Wall -Wextra -Wpedantic -O2
TARGET = mq_router

.PHONY: all clean run
all: $(TARGET)

$(TARGET): mq_router.c
	$(CC) $(CFLAGS) -o $@ $<

run: $(TARGET)
	./$(TARGET) router

clean:
	rm -f $(TARGET)
