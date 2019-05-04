import math
import mmh3 
from bitarray import bitarray 

''' 
    Class for Bloom filter, using murmur3 hash function 
    '''
class BloomFilter(object):

    def __init__(self, items_count, fp_prob):

        # False possible probability in decimal
        self.fp_prob = fp_prob 
  
        # Size of bit array to use 
        self.size = self.get_size(items_count,fp_prob) 
  
        # number of hash functions to use 
        self.hash_count = self.get_hash_count(self.size, items_count)
  
        # Bit array of given size 
        self.bit_array = bitarray(self.size) 
  
        # initialize all bits as 0 
        self.bit_array.setall(0) 

    '''
    Add an item in the filter 
    '''
    def add(self, item):
        digests = [] 
        for i in range(self.hash_count): 
  
            # create digest for given item. 
            # i work as seed to mmh3.hash() function 
            # With different seed, digest created is different 
            digest = mmh3.hash(item, i) % self.size
            digests.append(digest) 
  
            # set the bit True in bit_array 
            self.bit_array[digest] = True

    ''' 
        Check for existence of an item in filter 
        '''
    def check(self, item): 

        for i in range(self.hash_count): 
            digest = mmh3.hash(item,i) % self.size 
            if self.bit_array[digest] == False: 
  
                # if any of bit is False then,its not present 
                # in filter 
                # else there is probability that it exist 
                return False
        return True

    ''' 
            Return the size of bit array(m) to used using 
            following formula 
            m = -(n * lg(p)) / (lg(2)^2) 
            n : int 
                number of items expected to be stored in filter 
            p : float 
                False Positive probability in decimal 
            '''
    @classmethod
    def get_size(self,n,p): 

        m = -(n * math.log(p))/(math.log(2)**2) 
        return int(m)

    ''' 
            Return the hash function(k) to be used using 
            following formula 
            k = (m/n) * lg(2) 

            m : int 
                size of bit array 
            n : int 
                number of items expected to be stored in filter 
            '''
    @classmethod
    def get_hash_count(self, m, n): 

        k = (m/n) * math.log(2) 
        return int(k) 