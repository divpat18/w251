import zipfile
import cPickle as pickle

def preprocess():
    n_grams_dict={}
    print 'Starting'
    
    #Iterating over all files
    for file_count in range(0,2):
        z_file = 'google-'+str(file_count)+'.zip'
        print'Preprocessing File',z_file
        zipped_file = zipfile.ZipFile(z_file)
        
        #Iterating over all files found in zip
        for zip_info in zipped_file.infolist():
            i = int(0)
            #Opening each file to read
            with zipped_file.open(zip_info) as f:
                    while(1):
    
                        #Reading 100000 lines to imprve disk io
                        lines = f.readlines(100000)
                        if(len(lines) == 0):
                            break
                        for line in lines:
                            count =0
                            
                            #First letter of the two gram
                            first_lev_ngram = str.split(line)[0]
                            
                            #Second Letter of the two gram
                            second_lev_ngram = str.split(line)[1]
                            
                            #Number of occurences
                            ngram_count = str.split(line)[3]
                            if(first_lev_ngram in n_grams_dict):
                                sub_dict = n_grams_dict[first_lev_ngram]
                                if(second_lev_ngram in sub_dict):
                                    sub_dict[second_lev_ngram] = int(sub_dict[second_lev_ngram]) + int(ngram_count)
                                else:
                                    sub_dict[second_lev_ngram]=int(ngram_count)
                            else:
                                count = int(ngram_count)
                                sub_dict = {second_lev_ngram:count} 
                            n_grams_dict[first_lev_ngram] = sub_dict
        
        #Dumpimg into pickle file                    
        with open(z_file+'.pickle','wb') as pickle_file:
            pickle.dump(n_grams_dict,pickle_file)
        print'Completed File: ',z_file
        
    print 'All Preprocessing Complete'
    
preprocess()   
