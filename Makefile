PROJECT = minishard
NID := 1
SHELL_OPTS = -sname minishard$(NID) -setcookie minishard_demo
include erlang.mk
