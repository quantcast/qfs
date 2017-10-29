

all: dns.pdf # spf.pdf

%.pdf: %.tex
	@echo Pass 1
	@pdflatex $< >/dev/null </dev/null
	@echo Index
	@T="$<"; makeindex $${T%.tex}
	@T="$<"; makeglossaries $${T%.tex}
	@echo Pass 2
	@pdflatex $< >/dev/null </dev/null
	@echo Pass 3
	@pdflatex $< >/dev/null </dev/null
	@test -t 2 && open $@

.PHONY: clean clean~

clean:
#	rm -f *.aux *.log *.pdf *.ilg *.idx *.ind *.toc *.out
	find $(CURDIR) \( -type f -a -name dns.\* -a ! -name dns.tex \) -exec rm {} \;

clean~: clean
	rm -f *~
