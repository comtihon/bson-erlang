
all: clean compile xref eunit

clean compile xref eunit:
	@./rebar $@
