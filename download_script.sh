#!/bin/bash

case $1 in

gpfs1)
for i in {0..33}
do
echo $i
wget -O /gpfs/gpfsfpo/ngramData/google-2gram-$i.zip http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-all-2gram-20090715-$i.csv.zip
done
;;

gpfs2)
for i in {34..66}
do
echo $i
wget -O /gpfs/gpfsfpo/ngramData/google-2gram-$i.zip   http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-all-2gram-20090715-$i.csv.zip
done
;;

gpfs3)
for i in {67..99}
do
echo $i
wget -O /gpfs/gpfsfpo/ngramData/google-2gram-$i.zip  http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-all-2gram-20090715-$i.csv.zip
done
;;
*)
echo 'Specify correct hostname'
;;
esac
~       
