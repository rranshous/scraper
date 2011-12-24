import sys, os.path
# add the thrift stuff to our path
sys.path.insert(0,
    os.path.abspath(
        os.path.join(
            os.path.abspath(
                os.path.dirname(
                    __file__)),
            '../gen-py')))
