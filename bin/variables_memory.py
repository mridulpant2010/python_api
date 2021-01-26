import ctypes

def ref_count(address):
    return ctypes.c_long.from_address(address).value



if __name__=='__main__':

    mridul='123'
    ab=ref_count(id(mridul))
    print(ab)