#include <stdio.h>
#include <stdlib.h>

#include <err.h>

#include "spf.c"

int main(void) {
	unsigned char txt[2] = "";
	unsigned i, n, exp;

	for (i = 0; i < 256; i++) {
		txt[0] = i;
		n = spf_xtoi((char *)txt);

		switch (i) {
		case '0':
		case '1':
		case '2':
		case '3':
		case '4':
		case '5':
		case '6':
		case '7':
		case '8':
		case '9':
			exp = i - '0';
			break;
		case 'A':
		case 'B':
		case 'C':
		case 'D':
		case 'E':
		case 'F':
			exp = 10 + (i - 'A');
			break;
		case 'a':
		case 'b':
		case 'c':
		case 'd':
		case 'e':
		case 'f':
			exp = 10 + (i - 'a');
			break;
		default:
			exp = 0;
			break;
		}

		if (n != exp)
			errx(1, "expected %u, got %u", exp, n);
	}

	warnx("OK");

	for (i = 0; i < 1U << 20; i++) {
		unsigned r = (unsigned)random();
		char txt[20];
		unsigned n;

		if (snprintf(txt, sizeof txt, "%x", r) < 0)
			err(1, "snprintf");

		n = spf_xtoi(txt);
		if (r != n)
			errx(1, "expected 0x%x, got 0x%x (\"%s\")", r, n, txt);
	}

	return 0;
}
