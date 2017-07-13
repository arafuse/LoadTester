#!/usr/bin/env sh
# Convenience script for compiling with cython on my system, for speed. Feel free to adapt to your own use.

cython loadtester.py --embed
gcc -O2 -I /usr/include/python3.6m -o loadtester loadtester.c -lpython3.6m -lpthread -lm -lutil -ldl
