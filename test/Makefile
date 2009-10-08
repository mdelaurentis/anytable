# Makefile for testing command-line tools

anytable=../bin/anytable
input=../sample-data
actual=actual
expected=expected
tests=cut-breeds.tab.diff cut-breeds-inverted.tab.diff

all : $(tests)

$(actual)/cut-breeds.tab : $(input)/breeds.tab
	$(anytable) cut breed size <$< >$@

$(actual)/cut-breeds-inverted.tab : $(input)/breeds.tab
	$(anytable) cut -v category <$< >$@

%.diff : $(expected)/% $(actual)/%
	diff $^ > $@

clean :
	rm -f $(actual)/* *.diff