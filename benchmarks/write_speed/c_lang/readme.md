it appears that fwrite with a large buffer is the fastest...got it to go under 23 seconds

also, malloc is needed for large buffers or you get a segmentation fault error.

fastest run was with a buffer of 1MB, step_by 50 at 22.84 seconds
1mb = 1024 * 1024

curious if i were to make a step by 100 what that would do on perf