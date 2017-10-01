#!/bin/sh

SNAME=eb_pers
PA=$HOME/$SNAME
export HEART_COMMAND="$HOME/run_eb_pers.sh"
cd "$PA"

ROOTDIR=$PA
RELDIR=$PA/releases
DATA_FILE=$RELDIR/start_erl.data

ERTS_VSN=`awk '{print $1}' $DATA_FILE`
VSN=`awk '{print $2}' $DATA_FILE`

BINDIR=$ROOTDIR/erts-$ERTS_VSN/bin
EMU=beam
PROGNAME=$SNAME
export EMU
export ROOTDIR
export BINDIR
export PROGNAME
export RELDIR

exec $BINDIR/erlexec \
		-boot $RELDIR/$VSN/start \
		-sname $SNAME@localhost \
		-detached \
		-heart \
		-noinput \
		-s eb_pers run_app linux

