# Builds and runs example.c against the FireMQ C library.
#
#   make run                      # debug build of the library
#   make run PROFILE=release
#   make run FIREMQ_URL=fire://guest:guest@broker:5673

ROOT    ?= ../..
PROFILE ?= debug
TARGET_DIR ?= $(ROOT)/target
LIBDIR  ?= $(TARGET_DIR)/$(PROFILE)
CARGO_FLAGS = $(if $(filter release,$(PROFILE)),--release,)
CFLAGS  ?= -std=c11 -Wall -Wextra -Werror -O2

example: example.c firemq.h lib
	$(CC) $(CFLAGS) -I. -o $@ example.c -L$(LIBDIR) -lfiremq_client_ffi -Wl,-rpath,$(abspath $(LIBDIR))

lib:
	cargo build -p firemq-client-ffi $(CARGO_FLAGS) --manifest-path $(ROOT)/Cargo.toml --target-dir $(TARGET_DIR)

run: example
	./example

clean:
	rm -f example

.PHONY: lib run clean
