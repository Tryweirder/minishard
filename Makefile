PROJECT = minishard
COMPILE_FIRST = minishard_gen_leader

NID := 1
SHELL_OPTS = -sname minishard$(NID) -setcookie minishard_demo -s minishard -boot start_sasl -sasl errlog_type error

TEST_DEPS = detest

include erlang.mk
