#!/bin/bash
for FILE in `ls -t`
do
  echo ${FILE}
  for FILE2 in `ls -t ${FILE}`
  do
	  for FILE3 in `ls -t ${FILE}/${FILE2}`
	  do
			  if [[ ${FILE3} == test-9s-mdl-26clients-1-* ]] ;
			  then
				  #echo ${FILE2}
				  #echo PAXOS
				  mv ${FILE}/${FILE2} ${FILE}/mp
				  break
			  fi
			  if [[ ${FILE3} == test-9s-mdl-26clients-0-* ]] ;
			  then
				  #echo ${FILE2}
				  #echo MDL
				  mv ${FILE}/${FILE2} ${FILE}/mdl
				  break
			  fi
	  done
  done
done
