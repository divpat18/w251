import cPickle as pickle
import numpy as np
import sys
import paramiko
def get_all_ngrams(word):
    ngrams = {}
    with open('/gpfs/gpfsfpo/gpfs1/'+word[0].lower()+'.txt.pickle','rb') as pickle_file:
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

def mumble(word):
     
    ng = get_all_ngrams(word)
    
    #Base Case when no more ngrams are available
    if(ng is None):
        return None
    
    #Getting probabilities
    prob_words,prob = get_probabilities_for_word(ng)
    
    #Randon selection of next word based on calculated probabilities
    next_word = np.random.choice(prob_words,1,prob)
    
    #Returning Next Word
    #print next_word[0]
    return next_word[0]

def mumbler(word, max_count):
   
    #Base case when limit runs out
    if(max_count-1 ==0):
        print 'Exhausted max limit'
        return word
    
    #Base Case when no more ngrams are available
    if(word is None or len(word)==0):
        print "Out of Words"
        return 
    word = word.rstrip('\n')
    print word
    #Setup
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    stdin, stdout,stderr = None, None, None
    
    # Looking up the next word in the appropriate place
    if((word[0]>='a' and word[0]<='i') or (word[0]>='A' and word[0]<='I') ):
        #run mumble on local node with max count = 1
        word = mumble(word)
    
    
    elif((word[0]>='j' and word[0]<='r') or (word[0]>='J' and word[0]<='R') ):
        #Run Mumble on GPFS2 with max count 1
        ssh.connect('198.11.202.189',username='root',password='AZU2H6xz')
        stdin,stdout,stderr = ssh.exec_command('source activate pyt2.7 \n python /gpfs/gpfsfpo/gpfs2/mumble.py ' + word)
        word = stdout.readline()
        #print stderr.readlines()
    
    elif((word[0]>='s' and word[0]<='z') or (word[0]>='S' and word[0]<='Z') ):
        #Run Mumble on GPFS 3 with max count 1
        ssh.connect('198.11.202.190',username='root',password='An2Nvz6s')
        stdin,stdout,stderr = ssh.exec_command('source activate pyt2.7 \n python /gpfs/gpfsfpo/gpfs3/mumble.py ' + word)
        word = stdout.readline()
        #print stderr.readlines()
    
    mumbler(word,max_count-1)

mumbler(sys.argv[1],int(sys.argv[2]))
