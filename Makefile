PROJECT = minishard
NID := 1
SHELL_OPTS = -sname minishard$(NID) -setcookie minishard_demo -s minishard -boot start_sasl -sasl errlog_type error
include erlang.mk
