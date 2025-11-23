#include <serd/serd.h>
#include <iostream>
#include "include/serd_buffer.hpp"

// Requires a separate CmakeLists cmake --build build
int main() {
	SerdBuffer buffer("tests.trig", "http://example.org/base/");
	buffer.StartParse();
	while (!buffer.EverythingProcessed()) {
		RDFRow r = buffer.GetNextRow();
		cout << "s: " << r.subject << " p: " << r.predicate << " o: " << r.object << "\n";
	}
	return 0;
}
