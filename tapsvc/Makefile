all:		driver

driver.o:	driver.c
		gcc -c -I. -I/exo/cm/dev/base/include -g -Wall -D_FILE_OFFSET_BITS=64 driver.c

driver:		driver.o
		gcc -g -Wall -D_FILE_OFFSET_BITS=64 -o driver driver.o /exo/cm/dev/base/lib/libsvc.a

