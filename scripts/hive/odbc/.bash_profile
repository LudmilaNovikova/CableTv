# .bash_profile

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
	. ~/.bashrc
fi

# User specific environment and startup programs

PATH=$PATH:$HOME/bin

export PATH
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/lib64
export ODBCSYSINI=/etc
export ODBCINI=/etc/odbc.ini
export LD_PRELOAD=/usr/lib64/libodbcinst.so
