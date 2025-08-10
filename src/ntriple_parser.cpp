#include <iostream>
#include <fstream>
#include <string>
#include <cctype>
#include "parse_triple.hpp"

using namespace std;

// --- Main test program ---
// Compile with g++ -std=c++17 -O2 parse_triple.cpp ntriple_parser.cpp -o test_runner
int main(int argc, char *argv[]) {
	if (argc != 2) {
		cerr << "Usage: " << argv[0] << " file.nt\n";
		return 1;
	}

	ifstream infile(argv[1]);
	if (!infile.is_open()) {
		cerr << "Error: Could not open file " << argv[1] << "\n";
		return 1;
	}

	string subject, predicate, object, lang_tag, datatype_iri;
	string line;
	size_t line_num = 0;

	while (getline(infile, line)) {
		line_num++;
		if (line.empty() || line[0] == '#') {
			continue; // skip empty/comment lines
		}

		bool ok = ParseTripleLine(line, subject, predicate, object, lang_tag, datatype_iri);
		if (!ok) {
			cout << "Line " << line_num << ": Parse failed\n";
		} else {
			cout << "Line " << line_num << ":\n";
			cout << "  subject : " << subject << "\n";
			cout << "  predicate: " << predicate << "\n";
			cout << "  object  : " << object << "\n";
			if (!lang_tag.empty())
				cout << "  lang_tag: " << lang_tag << "\n";
			if (!datatype_iri.empty())
				cout << "  datatype: " << datatype_iri << "\n";
		}
		cout << "---------------------------------\n";
	}

	return 0;
}
