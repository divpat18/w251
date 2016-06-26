import object_storage
import datetime
sl_storage = object_storage.get_client('SLOS1011257-2:SL1011257','73b75a2cd3b2345903da72d33c6465a01c1dbf9e25acb635b79b6965ba67607c', datacenter='dal05')

sl_storage.containers()

sl_storage['foo'].properties

start = datetime.datetime.now()
print 'Started Upload'

end = datetime.datetime.now()

print start, end
read_start = datetime.datetime.now()
sl_storage['foo']['google-0.csv'].save_to_filename('google-0.csv')
read_end = datetime.datetime.now()

print read_start, read_end


