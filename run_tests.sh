#! /bin/bash

ulimit -c unlimited

p=1
while [ true ]
do
		#-----------------------------------------------------------------------
		# ld_bd tests
		#-----------------------------------------------------------------------
		echo `date`: ./kdb/c_src/tests.linux/ld_bd_test.debug $p...
		./kdb/c_src/tests.linux/ld_bd_test.debug > /var/ramdisk/log.txt
		if [ $? -ne 0 ]
		then
				mplayer -quiet /usr/lib/openoffice/share/gallery/sounds/romans.wav
				echo `date`: "**** TEST FAILED ****"
				exit 1
		fi
		echo `date`: ld_bd_test.debug passed $p

		echo `date`: ./kdb/c_src/tests.linux/ld_bd_test.release $p...
		./kdb/c_src/tests.linux/ld_bd_test.release > /var/ramdisk/log.txt
		if [ $? -ne 0 ]
		then
				mplayer -quiet /usr/lib/openoffice/share/gallery/sounds/romans.wav
				echo `date`: "**** TEST FAILED ****"
				exit 1
		fi
		echo `date`: ld_bd_test.release passed $p

		#-----------------------------------------------------------------------
		# ld_pp tests
		#-----------------------------------------------------------------------
		echo `date`: ./kdb/c_src/tests.linux/ld_pp_test.debug $p...
		./kdb/c_src/tests.linux/ld_pp_test.debug > /var/ramdisk/log.txt
		if [ $? -ne 0 ]
		then
				mplayer -quiet /usr/lib/openoffice/share/gallery/sounds/romans.wav
				echo `date`: "**** TEST FAILED ****"
				exit 1
		fi
		echo `date`: ld_pp_test.debug passed $p

		echo `date`: ./kdb/c_src/tests.linux/ld_pp_test.release $p...
		./kdb/c_src/tests.linux/ld_pp_test.release > /var/ramdisk/log.txt
		if [ $? -ne 0 ]
		then
				mplayer -quiet /usr/lib/openoffice/share/gallery/sounds/romans.wav
				echo `date`: "**** TEST FAILED ****"
				exit 1
		fi
		echo `date`: ld_pp_test.release passed $p

		#-----------------------------------------------------------------------
		# stm 1 tests
		#-----------------------------------------------------------------------
		echo `date`: ./kdb/c_src/tests.linux/stm_test_1.debug $p...
		./kdb/c_src/tests.linux/stm_test_1.debug > /var/ramdisk/log.txt
		if [ $? -ne 0 ]
		then
				mplayer -quiet /usr/lib/openoffice/share/gallery/sounds/romans.wav
				echo `date`: "**** TEST FAILED ****"
				exit 1
		fi
		echo `date`: stm_test_1.debug passed $p

		echo `date`: ./kdb/c_src/tests.linux/stm_test_1.release $p...
		./kdb/c_src/tests.linux/stm_test_1.release > /var/ramdisk/log.txt
		if [ $? -ne 0 ]
		then
				mplayer -quiet /usr/lib/openoffice/share/gallery/sounds/romans.wav
				echo `date`: "**** TEST FAILED ****"
				exit 1
		fi
		echo `date`: stm_test_1.release passed $p

		#-----------------------------------------------------------------------
		# stm 2 tests
		#-----------------------------------------------------------------------
		echo `date`: ./kdb/c_src/tests.linux/stm_test_2.debug $p...
		./kdb/c_src/tests.linux/stm_test_2.debug > /var/ramdisk/log.txt
		if [ $? -ne 0 ]
		then
				mplayer -quiet /usr/lib/openoffice/share/gallery/sounds/romans.wav
				echo `date`: "**** TEST FAILED ****"
				exit 1
		fi
		echo `date`: stm_test_2.debug passed $p

		echo `date`: ./kdb/c_src/tests.linux/stm_test_2.release $p...
		./kdb/c_src/tests.linux/stm_test_2.release > /var/ramdisk/log.txt
		if [ $? -ne 0 ]
		then
				mplayer -quiet /usr/lib/openoffice/share/gallery/sounds/romans.wav
				echo `date`: "**** TEST FAILED ****"
				exit 1
		fi
		echo `date`: stm_test_2.release passed $p

		#-----------------------------------------------------------------------
		# btree 1 tests
		#-----------------------------------------------------------------------
		echo `date`: ./kdb/c_src/tests.linux/btree_test_1.debug $p...
		./kdb/c_src/tests.linux/btree_test_1.debug > /var/ramdisk/log.txt
		if [ $? -ne 0 ]
		then
				mplayer -quiet /usr/lib/openoffice/share/gallery/sounds/romans.wav
				echo `date`: "**** TEST FAILED ****"
				exit 1
		fi
		echo `date`: btree_test_1.debug passed $p

		echo `date`: ./kdb/c_src/tests.linux/btree_test_1.release $p...
		./kdb/c_src/tests.linux/btree_test_1.release > /var/ramdisk/log.txt
		if [ $? -ne 0 ]
		then
				mplayer -quiet /usr/lib/openoffice/share/gallery/sounds/romans.wav
				echo `date`: "**** TEST FAILED ****"
				exit 1
		fi
		echo `date`: btree_test_1.release passed $p

		p=$((p+1))
done
