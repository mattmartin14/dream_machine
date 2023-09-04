it appears that fwrite with a large enough buffer is the fastest...got it to go under 23 seconds

also, malloc is needed for large buffers or you get a segmentation fault error.

fastest run was with a buffer of 1MB, step_by 50 at 22.84 seconds
1mb = 1024 * 1024

with a buffer of 1MB and writing blocks of 100 at a time, it runs in 22.5 seconds