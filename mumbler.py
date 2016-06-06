import zipfile
import cPickle as pickle
import numpy as np
from array import *
import sys

def get_all_ngrams(word):
    ngrams = {}
    for file_index in range(0,2):
        #Going through all the pickle files and collecting the data for given word
        if(file_index % 25==0):
            print 'Looking through file: ',file_index,' for word: ',word
        with open('google-'+str(file_index)+'.zip'+'.pickle','rb') as pickle_file:
            unpickled = pickle.load(pickle_file)
            #Finding Second level n grams
            temp_dict=unpickled.get(word)
            if(temp_dict is not None):
                #Collecting Counts
                for key in temp_dict:
                    if(key in ngrams):
                        ngrams[key] = int(ngrams[key]) + int(temp_dict[key])
                    else:
                        ngrams[key]=int(temp_dict[key])
    if (len(ngrams)==0):
        return None
    else:
        return ngrams


#Returns the associated words and their probabilities
def get_probabilities_for_word(ng):
    #Computing Sum
    sum = 0.0
    for key in ng:
        sum = sum + float(ng[key])
    probability_dict={}
    #Computing Probabilities
    for key in ng:
        probability_dict[key] = float(ng[key]/sum)
    return probability_dict.keys(),probability_dict.values()


def mumble(word, limit):
    
    #Base case when limit runs out
    print 'Looking up: ',word
    if(limit-1 ==0):
        print 'Exhausted max limit'
        return
    
    ng = get_all_ngrams(word)
    
    #Base Case when no more ngrams are available
    if(ng is None):
        print 'No more words found'
        return
    
    #Getting probabilities
    prob_words,prob = get_probabilities_for_word(ng)
    
    #Randon selection of next word based on calculated probabilities
    next_word = np.random.choice(prob_words,1,prob)
    
    #Mumling for next word
    return mumble(next_word[0],limit-1)


mumble(str(sys.argv[1]),int(sys.argv[2]))
