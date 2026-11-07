# Builds and runs example.c against the FireMQ C library.
#
#   make run                      # builds the library (debug) and the example, then runs it
#   make run PROFILE=release
#   make example LIBDIR=/path/to/dir/with/libfiremq_client_ffi   # use a prebuilt library
#
# The example connects to $FIREMQ_URL (default fire://guest:guest@localhost:5673).

ROOT    ?= ../..
PROFILE ?= debug
TARGET_DIR ?= $(ROOT)/target
LIBDIR  ?= $(TARGET_DIR)/$(PROFILE)
CARGO_FLAGS = $(if $(filter release,$(PROFILE)),--release,)
CFLAGS  ?= -std=c11 -Wall -Wextra -Werror -O2

example: example.c firemq.h
	$(CC) $(CFLAGS) -I. -o $@ example.c -L$(LIBDIR) -lfiremq_client_ffi -Wl,-rpath,$(abspath $(LIBDIR))

lib:
	cargo build -p firemq-client-ffi $(CARGO_FLAGS) --manifest-path $(ROOT)/Cargo.toml --target-dir $(TARGET_DIR)

run: lib example
	./example

clean:
	rm -f example

.PHONY: lib run clean
