import cPickle as pickle

def alphabetizer():
	#Looping through the files.
	for i in range(100):
		print 'File',i
		with open('ngramData/google-2gram-'+ str(i) +'.txt.pickle') as pickle_file:
			unpickled = pickle.load(pickle_file)
			#First Level
			for data in unpickled:
				data_count = unpickled.get(data)
				#Second Level
				for sec_level in data_count:
					file_name = data[0][0].lower()+'.txt'
					if((data>='a' and data<='i') or (data>='A' and data<='I') ):
						with open('gpfs1/'+file_name, 'a')as gpfs_file:
							gpfs_file.write(data + ' '+ sec_level + '	' + str(data_count[sec_level])+'\n')
					elif((data>='j' and data<='r') or (data>='J' and data<='R') ):
						#print 'Node 2 ' + file_name
						with open('gpfs2/'+file_name, 'a')as gpfs_file:
                                                        gpfs_file.write(data + ' '+ sec_level + '       ' + str(data_count[sec_level])+'\n')
					elif((data>='s' and data<='z') or (data>='S' and data<='Z') ):
                                                #print 'Node 3' + file_name
						with open('gpfs3/'+file_name, 'a')as gpfs_file:
                                                        gpfs_file.write(data + ' '+ sec_level + '       ' + str(data_count[sec_level])+'\n')

	print 'Done'

