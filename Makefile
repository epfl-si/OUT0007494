.PHONY: help
help:
	echo "Usage:"
	echo "     $(MAKE)"
	echo "     $(MAKE) mrproper"

.PHONY: all
all:
	Rscript analyse.R

.PHONY: mrproper
mrproper:
	-rm *.db *.jsonp
