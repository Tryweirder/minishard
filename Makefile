PROJECT = minishard
COMPILE_FIRST = minishard_gen_leader

NID := 1
SHELL_OPTS = -sname minishard$(NID) -setcookie minishard_demo -s minishard -boot start_sasl -sasl errlog_type error

BUILD_DEPS = elvis_mk
DEP_PLUGINS = elvis_mk
TEST_DEPS = detest

dep_elvis_mk = git https://github.com/inaka/elvis.mk.git 784e41bcb91

include erlang.mk
