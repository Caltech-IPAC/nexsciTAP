all: ctest dbdemo

ctest: ctest.c
	gcc -o ctest ctest.c -I/home/jcg/Anaconda/3.11_2023.09/include/python3.11 /home/jcg/Anaconda/3.11_2023.09/lib/libpython3.11.so.1.0

dbdemo: dbdemo.c
	gcc -o dbdemo dbdemo.c -I/home/jcg/Anaconda/3.11_2023.09/include/python3.11 /home/jcg/Anaconda/3.11_2023.09/lib/libpython3.11.so.1.0

clean:
	rm -f ctest dbdemo
