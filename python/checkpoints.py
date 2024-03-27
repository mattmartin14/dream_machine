"""
Author: Matt Martin
Date: 3/25/24
Desc: Demonstrates how to use labels/checkpoints for error tracking
"""

checkpoint = "start"

try:

    x, y, z = 4, 2, 0

    checkpoint = "step 1: add some numbers"
    res = x+y

    checkpoint = "step 2: multiply some numbers"
    res = x*y

    checkpoint = "step 3: divide some numbers"
    res = y/z
    
except Exception as e:
    err_msg = "Error occurred at checkpoint <<< [{0}] >>> \n".format(checkpoint)
    err_msg += "------------------------\n"
    err_msg += "Actual Error Message:\n\t <<< [{0}] >>> ".format(str(e))
    #print(err_msg)
    raise Exception (err_msg)